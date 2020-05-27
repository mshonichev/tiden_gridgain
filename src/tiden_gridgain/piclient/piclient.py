from threading import Lock
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from enum import Enum

from time import sleep

from tiden.ignite import Ignite
from tiden.tidenexception import TidenException
from tiden.logger import get_logger
from tiden.util import log_print, log_put, print_red, get_jvm_options
from tiden.apps.nodestatus import NodeStatus
from tiden.report.steps import step

try:
    from py4j.java_gateway import JavaGateway, GatewayParameters
except ImportError:
    print_red('Could not import JavaGateway, GatewayParameters from py4j.java_gateway')
    JavaGateway = None
    GatewayParameters = None

try:
    from py4j.protocol import Py4JJavaError as GatewayException
except ImportError:
    print_red('Could not import JavaGateway, GatewayParameters from py4j.java_gateway')
    GatewayException = Exception

# NB: this must be in sync with IgniteNodesMixin.MAX_NODE_START_ID
JMX_START_NODE_ID = 100000
MAX_PICLIENT_NODES = 1000


class PiClientType(Enum):
    IGNITE = 1
    CONSOLE = 2
    JMX = 3

    def get_node_log(self, remote_home, node_id, ignite: Ignite = None, host=None, name=None):
        """
        Create node log name and path

        :param remote_home:     default remote home
        :param node_id:         created node id
        :param ignite:          cluster options
        :param host:            host where client should be running
        :param name:            custom file name
        :return:                file path
        """
        custom_name = f'{name.replace(" ", "_")}.' if name else ''

        grid_prefix = '.1' if ignite is None else '.' + str(ignite.grid_name)
        types = {
            PiClientType.IGNITE.value: 'node',
            PiClientType.JMX.value: 'jmx',
            PiClientType.CONSOLE.value: 'console'
        }
        node_type = types.get(self.value)
        if node_type:
            full_path = f'{remote_home}/grid{grid_prefix}.{node_type}.{node_id}.{{}}.{custom_name}log'
            inc = 0
            while 'head: cannot open' not in ignite.ssh.exec_on_host(host, [f'head -n 1 {full_path.format(inc)}'])[host][0]:
                inc += 1
                if inc > 1000:
                    raise PiClientException("Failed to find free name")
            return full_path.format(inc)
        else:
            raise PiClientException("Unknown node type")

    def get_start_node_id(self, ignite):
        if PiClientType.IGNITE.value == self.value:
            return ignite.get_start_client_idx() - 1
        elif PiClientType.JMX.value == self.value:
            return JMX_START_NODE_ID
        else:
            raise PiClientException('Unknown type')


class PiClient:
    """
    Context wrapper for piclient-tiden

    Can be nested - in case of with: with: block - it doesn't start new instance (if doesn't select new instance kwarg)
    and use previously created piclients. All current piclients will be killed of nested = False

    In case of new instance it is store information about previous instance and use while quit wrapped wait block

    E.g.:
    1.
        with Piclient(ignite, config, nodes_num):
            -- start instance
            with Piclient(ignite, config, nodes_num): # nodes num for wrapped PiClient will be ignored
                -- use previous instance

    2.
        with Piclient(ignite, config, nodes_num)
            -- start instance
            with Piclient(ignite, config, nodes_num, new_instance=True)
                -- starts new instance and new client

            -- return parent instance to flow (work with parent client)

    Works with methods too (calculate checksums)
    """
    current_counter_ignite = 0
    current_counter_gateway = 0

    nested = False
    previous_instance = None

    # Hidden instance variables:
    #
    # piclients = {}
    # nodes_num = 0
    # node_ids = ()
    #
    # clear = True

    def __init__(self, ignite_cluster, config, **kwargs):
        """

        :param ignite_cluster: Ignite instance
        :param config: config
        :param kwargs:
            nodes_num: number of nodes to start.
                If None - start up to client_hosts * clients_per_host,
                or use same number as previous PiClient for wrapped PiClient instances
            client_hosts: use only subset of environment client hosts to run nodes
            clients_per_host: allows to override environment clients_per_host
            clear: clear PiClient node information from Ignite.nodes after PiClient shutdown
            jvm_options: allows to override started nodes JVM options
            new_instance: force create new PiClient instance, e.g. don't reuse ones already existing
            read_timeout: allows to override default Py4J gateway read timeout
        """
        # check current thread for PiClient instance
        if getattr(threading.current_thread(), 'piclient-instance', None) and not kwargs.get('new_instance'):
            self.nested = True

            instance = getattr(threading.current_thread(), 'piclient-instance')

            self.ignite_cluster = instance.ignite_cluster

            if 'nodes_num' in kwargs and \
                    kwargs['nodes_num'] is not None and \
                    kwargs['nodes_num'] != instance.nodes_num:
                print_red("Wrapped PiClient code has different number of nodes! Using previous nodes number: %s" %
                          instance.nodes_num)

            self.nodes_num = instance.nodes_num
            self.node_ids = instance.node_ids

            self.piclients = instance.piclients
        else:
            self.nested = False

            if getattr(threading.current_thread(), 'piclient-instance', None):
                self.previous_instance = getattr(threading.current_thread(), 'piclient-instance')

            self.ignite_cluster = ignite_cluster

            self.nodes_num = kwargs.get('nodes_num', None)
            if self.nodes_num is None:
                client_hosts = kwargs.get('client_hosts', None)
                clients_per_host = kwargs.get('clients_per_host', None)

                self.nodes_num = self.__get_num_client_nodes(client_hosts, clients_per_host)

            self.starter = PiClientStarter(ignite_cluster, read_timeout=kwargs.get('read_timeout', None))
            self.node_ids = self.starter.start_common_nodes(config, self.nodes_num, PiClientType.IGNITE, **kwargs)

            self.piclients = {}
            for node_id in self.node_ids:
                self.piclients[node_id] = {}
                self.piclients[node_id]['gateway'] = self.ignite_cluster.nodes[node_id]['gateway']

            # skip ignite registration if no need wait to start ignition
            if kwargs.get('wait_for_ignition_start', True):
                self.register_ignite()

        self.clear = kwargs.get('clear', True)

    def __enter__(self):
        setattr(threading.current_thread(), 'piclient-instance', self)

        return self

    def __exit__(self, etype, value, tb):
        if etype is None:
            self.__clear_instance_with_killing_nodes()

            # return previous instance to thread.local
            if self.previous_instance:
                setattr(threading.current_thread(), 'piclient-instance', self.previous_instance)
        else:
            if isinstance(etype, TidenException):
                self.__clear_instance_with_killing_nodes()
                raise value
            elif not isinstance(etype, AssertionError):
                log_print("Exception:\n", color='red')
                traceback.print_exception(etype, value, tb)

                self.__clear_instance_with_killing_nodes()
                raise PiClientException("Something wrong during piclient evaluating")
            else:
                self.__clear_instance_with_killing_nodes()

    def __clear_instance_with_killing_nodes(self):
        # kill nodes if it's not a nested piclient
        if not self.nested:
            setattr(threading.current_thread(), 'piclient-instance', None)
            for node_id in self.piclients.keys():
                self.starter.shutdown(node_id, clear=self.clear)

    def __get_num_client_nodes(self, client_hosts=None, clients_per_host=None):
        if client_hosts is None:
            client_hosts = self.ignite_cluster.config['environment']['client_hosts']
        if clients_per_host is None:
            if 'clients_per_host' in self.ignite_cluster.config['environment']:
                clients_per_host = int(self.ignite_cluster.config['environment']['clients_per_host'])
            else:
                clients_per_host = 1

        return len(client_hosts) * clients_per_host

    def register_ignite(self):
        for node_id in self.node_ids:
            self.piclients[node_id]['ignite'] = self.ignite_cluster.nodes[node_id]['gateway'] \
                .entry_point.getIgniteService().getIgnite()

    def get_node_id(self):
        """
        Get next node_id in piclient instance. Increments next counter.
        (Generator principles used)

        :return: node_id
        """
        for counter, node_id in enumerate(self.piclients.keys()):
            if counter == self.current_counter_ignite:
                # drop counter to value by mod self.nodes_num to prevent massive counter grow
                self.current_counter_ignite = (self.current_counter_ignite + 1) % self.nodes_num
                return node_id

    def get_ignite(self, node_id=None):
        """
        Get ignite instance from piclients
        If node_id is not defined then next ignite instance will be returned
        (Generator principles used)

        :param node_id: returns ignite instance for node_id
        :return: Ignite.class Java instance
        """
        if node_id:
            if node_id not in self.piclients:
                raise PiClientException('Unknown node id %s' % node_id)

            return self.piclients[node_id]['ignite']

        for counter, piclient in enumerate(self.piclients.values()):
            if counter == self.current_counter_ignite:
                # drop counter to value by mod self.nodes_num to prevent massive counter grow
                self.current_counter_ignite = (self.current_counter_ignite + 1) % self.nodes_num
                return piclient['ignite']

    def get_gateway(self, node_id=None):
        """
        Get gateway instance from piclients
        If node_id is not defined then next gateway instance will be returned
        (Generator principles used)

        :param node_id: returns gateway instance for node_id
        :return: py4j gateway
        """
        if node_id:
            if node_id not in self.piclients:
                raise PiClientException('Unknown node id %s' % node_id)

            return self.piclients[node_id]['gateway']

        for counter, piclient in enumerate(self.piclients.values()):
            if counter == self.current_counter_gateway:
                self.current_counter_gateway = (self.current_counter_gateway + 1) % self.nodes_num
                return piclient['gateway']


class PiClientStarter:
    lock = Lock()

    piclient_gateway_port_counter = 0
    piclient_gateway_port_base = 25333
    piclient_gateway_range = 100

    def __init__(self, ignite, start_gateway=True, read_timeout=None):
        """
        :param ignite: instance of pt.ignite.Ignite
        """
        self.read_timeout = 60*30 if read_timeout is None else read_timeout
        self.ignite = ignite
        self.logger = get_logger('tiden')

        self.start_gateway = start_gateway

    def get_libs(self):
        return self.ignite.get_libs()

    @step('Start {num} {piclient_type} nodes')
    def start_common_nodes(self, config=None, num=1, piclient_type=PiClientType.IGNITE, **kwargs):
        """
        Start piclient nodes

        :param config: config of newly added nodes
        :param num: number of newly added nodes
        :param piclient_type: node type to start
        :param kwargs:
            jvm_options - remote JVM start options
            client_hosts - use subset of client hosts to start to select nodes instead of all client hosts
        :return:
        """
        PiClientStarter.lock.acquire()
        started_node_ids = []
        try:
            started_node_ids = self.calculate_start_ids(num, piclient_type.get_start_node_id(self.ignite))
            ignite_name = self.ignite.grid_name
            if kwargs.get('name'):
                ignite_name += f' ({kwargs["name"]})'
            print_config = f", config: {config}" if config else ''
            log_print(f"[piclient]: starting grid {ignite_name} nodes {list(started_node_ids)}, type: {piclient_type}{print_config}")

            launch_commands = self.create_launch_commands(piclient_type, started_node_ids, **kwargs)
            self.ignite.ssh.exec(launch_commands)

            self.wait_jvms_to_start(started_node_ids)
            self.register_started_nodes(piclient_type, started_node_ids, **kwargs)

            if piclient_type == PiClientType.IGNITE:
                self.parallel_ignition_start(config, started_node_ids, **kwargs)
        finally:
            PiClientStarter.lock.release()
        return list(started_node_ids)

    def calculate_start_ids(self, num, start_id):
        """
        Thread safe getting node ids (to avoid different threads take same node ids)
        Other code shouldn't depends on different threads

        :param num: number of id's to lock
        :param start_id: start id
        :return: range of ids to start
        """

        start_node_index = start_id + 1
        while self.ignite.nodes.get(start_node_index) is not None and \
                start_node_index < start_id + MAX_PICLIENT_NODES + 1:
            start_node_index += 1
        started_node_ids = range(start_node_index, start_node_index + num)

        return list(started_node_ids)

    def create_launch_commands(self, piclient_type, started_node_ids, **kwargs):
        """
        Create node start command

        :param piclient_type: node type to start
        :param started_node_ids: node id to start
        :param kwargs:
            jvm_options - remote JVM start options
            client_hosts - use subset of client hosts to start to select nodes instead of all client hosts
        :return: clients - list of start command
        """
        clients = {}

        # Set working directory
        remote_home = self.ignite.config['rt']['remote']['test_dir']

        # Construct class path
        class_paths = []
        for lib_dir in self.get_libs():
            class_paths.append("%s/%s/*" % (self.ignite.client_ignite_home, lib_dir))
        class_paths.append(self.ignite.config['artifacts']['piclient']['remote_path'])

        # Construct jvm options
        config_jvm_options = get_jvm_options(self.ignite.config['environment'], 'client_jvm_options')
        jvm_options = get_jvm_options(kwargs, 'jvm_options')
        jvm_options_str = ' '.join(jvm_options + config_jvm_options)

        for node_id in started_node_ids:
            if kwargs.get('host', False):
                host = kwargs.get('host')
            else:
                # Get next common host
                host = self.ignite.get_and_inc_client_host(client_hosts=kwargs.get('client_hosts', None))

            # Client command line
            log_file = piclient_type.get_node_log(
                remote_home=remote_home,
                node_id=node_id,
                ignite=self.ignite,
                host=host,
                name=kwargs.get('name', None))

            gateway_port = self.get_piclient_gateway_port()
            cmd = f"cd {self.ignite.client_ignite_home}; " \
                  f"nohup " \
                  f"  $JAVA_HOME/bin/java " \
                  f"    -cp {':'.join(class_paths)} {jvm_options_str} " \
                  f"    -DCONSISTENT_ID={self.ignite.get_node_consistent_id(node_id)} " \
                  f"    -DNODE_IP={host} " \
                  f"    -DNODE_COMMUNICATION_PORT={self.ignite.get_node_communication_port(node_id)} " \
                  f"    -DIGNITE_QUIET=false " \
                  f"  org.apache.ignite.piclient.ClientEntryPoint " \
                  f"    --port={gateway_port}" \
                  f"> {log_file} 2>&1 &"

            log_print("create_launch_commands: will start node %d at host %s" % (node_id, host))

            self.ignite.nodes[node_id] = {
                'host': host,
                'ignite_home': self.ignite.client_ignite_home,
                'log': log_file,
                'status': NodeStatus.STARTING
            }

            if host not in clients:
                clients[host] = [cmd]
            else:
                clients[host].append(cmd)

        return clients

    def wait_jvms_to_start(self, started_node_ids):
        timeout_counter = 0
        completed = False
        launch_clients = {}
        # build cmd for status
        for node_id in started_node_ids:
            cmd = 'cat %s | grep "py4j gateway started"' % self.ignite.nodes[node_id]['log']

            node_host = self.ignite.nodes[node_id]['host']

            if node_host not in launch_clients:
                launch_clients[node_host] = [cmd]
            else:
                launch_clients[node_host].append(cmd)
        while timeout_counter < self.ignite.activation_timeout and not completed:
            results = self.ignite.ssh.exec(launch_clients)

            nodes_started = 0
            for host in results.keys():
                for out_lines in results[host]:
                    if 'py4j gateway started' in out_lines:
                        nodes_started += 1

            log_put("Waiting piclient JVMs to start %s/%s" % (nodes_started, len(started_node_ids)))
            if nodes_started == len(started_node_ids):
                completed = True
                continue

            sleep(5)
            timeout_counter += 5

    def register_started_nodes(self, piclient_type, started_node_ids, **kwargs):
        """
        Initializes piclient backend gateways.
        :param piclient_type:
        :param started_node_ids:
        :param kwargs:
            read_timeout - gateway communication timeout.
        :return:
        """
        timeout = kwargs.get('read_timeout', self.read_timeout)
        # Register piclient nodes in self.nodes
        self.ignite.get_pids(started_node_ids)
        self.ignite.get_data_from_log(
            started_node_ids,
            'py4j gateway port:',
            'py4j gateway port: (\d+)',
            'gateway_port',
            force_type='int'
        )

        for node_id in started_node_ids:
            log_print("PiClient %s type %s started JVM on %s:%s with PID %s " % (
                str(node_id), piclient_type, self.ignite.nodes[node_id]['host'],
                self.ignite.nodes[node_id]['gateway_port'],
                self.ignite.nodes[node_id]['PID']))
            try:
                if self.start_gateway:
                    gateway = JavaGateway(
                        gateway_parameters=GatewayParameters(address=self.ignite.nodes[node_id]['host'],
                                                             port=int(self.ignite.nodes[node_id]['gateway_port']),
                                                             read_timeout=timeout))

                    self.ignite.nodes[node_id]['gateway'] = gateway

                self.ignite.nodes[node_id]['status'] = NodeStatus.STARTED
            except GatewayException as gwexc:
                raise PiClientException(gwexc)

        if piclient_type == PiClientType.IGNITE:
            self.ignite.get_jmx_port('client')

    def parallel_ignition_start(self, config, started_node_ids, **kwargs):
        def ignite_start_worker(ignite_node_id, ignite_config):
            """thread worker function"""
            try:
                test_module_dir = self.ignite.config['rt']['remote']['test_module_dir']
                gateway = self.ignite.nodes[ignite_node_id]['gateway']
                gateway.entry_point.getIgniteService().startIgniteClientNode(f"{test_module_dir}/{ignite_config}")
            except Exception as e:
                if kwargs.get('exception_print', True):
                    print_red("Got Exception starting client: %s" % str(e))
                raise PiClientException(e)

        try:
            number_of_started_clients = len(started_node_ids)

            log_print("Start %s Ignite on above JVM %s" %
                      (number_of_started_clients,
                       'in parallel' if number_of_started_clients > 1 else 'in single thread'
                       )
                      )

            if number_of_started_clients > 1:
                executor = ThreadPoolExecutor()

                for node_id in started_node_ids:
                    executor.submit(ignite_start_worker, node_id, config)

                executor.shutdown(wait=kwargs.get('wait_for_ignition_start', True))
            else:
                ignite_start_worker(started_node_ids[0], config)
        except Exception as e:
            print_red('Exception occurs while starting Ignition.start() for PiClient node(s)')
            raise PiClientException(e)

    @staticmethod
    def get_piclient_gateway_port():
        PiClientStarter.piclient_gateway_port_counter = PiClientStarter.piclient_gateway_port_counter + 1

        return PiClientStarter.piclient_gateway_port_base + \
            (PiClientStarter.piclient_gateway_port_counter % PiClientStarter.piclient_gateway_range)

    def shutdown(self, node_index, gracefully=False, clear=False, start_gateway=True):
        if start_gateway:
            if gracefully:
                self.ignite.nodes[node_index]['gateway'].entry_point.shutdownGracefully()

            # Close connections and shutdown gateway
            self.ignite.nodes[node_index]['gateway'].shutdown()
            del self.ignite.nodes[node_index]['gateway']

        del self.ignite.nodes[node_index]['gateway_port']

        self.ignite.kill_node(node_index)

        if clear:
            del self.ignite.nodes[node_index]


def get_gateways():
    piclient_instance = getattr(threading.current_thread(), 'piclient-instance')

    if piclient_instance:
        gateways = []
        for piclient in piclient_instance.piclients.values():
            gateways.append(piclient['gateway'])

        return gateways
    else:
        raise PiClientException("There in no py4j gateway in context")


def get_gateway(gateway=None):
    """
    Static get_gateway method that takes gateway from args or threadlocal context

    If gateway defined then this gateway will be returned
    Otherwise it will be taken from threadlocal

    If there is no gateway in context or args - exception will be thrown

    :param gateway:
    :return:
    """

    if gateway:
        return gateway

    piclient_instance = getattr(threading.current_thread(), 'piclient-instance')
    if piclient_instance:
        return piclient_instance.get_gateway()
    else:
        raise PiClientException("There in no py4j gateway in context")


class PiClientException(Exception):
    pass
