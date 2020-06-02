#!/usr/bin/env python3
#
# Copyright 2017-2020 GridGain Systems.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from os.path import basename
from re import search
from time import time, sleep

from tiden.apps import NodeStatus
from tiden.apps.ignite import Ignite
from tiden.assertions import tiden_assert
from tiden.dockermanager import DockerManager
from tiden.util import log_put, log_print

from .dockermixin import DockerMixin
from .nodeconfig import NodeConfig


class Ignitedocker(Ignite, DockerMixin):

    def __init__(self, config, ssh, image_name='java', image_tag='8-jdk', name='Ignite Docker'):
        Ignite.__init__(self, name, config, ssh)
        self.docker = DockerManager(config, ssh)
        self.image_name = image_name
        self.image_tag = image_tag
        self.ignite_home = 'export IGNITE_HOME=/ignite; '
        self.docker_ssh = None

    def pull_images(self):
        self.ssh.exec([f'{self.image_name}:{self.image_tag}'])

    def start(self, configs, clean_before=True):
        """
        Start cluster

        :param configs:         list(NodeConfig): nodes configurations
        :param clean_before:    clean hosts before cluster run
        """
        log_print('Starting grid', color='green')
        from pt.sshpool import SshPool
        self.docker_ssh = SshPool(self.config['ssh'])
        self.docker_ssh.connect()
        self.docker_ssh.exec = self.exec_ignite_in_containers
        self.docker_ssh.exec_on_host = self.exec_ignite_in_container

        self.nodes = {}
        if clean_before:
            self.clean_all_hosts()

        self.pull_images()

        if configs[0].network not in [None, 'host']:
            self.docker.init_swarm()
            self.docker.create_swarm_network(configs[0].network, 'overlay')

        for config in configs:
            self.start_node_container(host=config.server_host, node_config=config, expected_servers_num=len(self.nodes) + 1)

    def get_deploy_data(self, configs, network):
        services = {}
        image = '{}:{}'.format(self.image_name, self.image_tag)
        for idx, config in enumerate(configs):
            hosts = self.ssh.hosts
            while idx + 1 > len(hosts):
                hosts = hosts + self.ssh.hosts
            host = hosts[idx]
            services[f'ignite-{idx}'] = {
                'depends_on': [network],
                'image': image,
                'networks': [config.network],
                'deploy': {
                    'replicas': 1,
                    'placement': {
                        'constraints': ['node.hostname == lab{}.gridgain.local'.format(host.split('.')[-1:][0])]
                    }
                },
                'command': 'bash -c "touch /general.log && tail -f /general.log"'
            }
        data = {
            'version': '3',
            'services': services,
            'networks': {
                network: {
                    'driver': 'overlay'
                }
            }
        }
        return data

    def start_node_container(self, host, node_config: NodeConfig, wait_after=True,
                             expected_servers_num=None, expected_clients_num=None, **kwargs):
        """
        1. Start base container from base image
        2. Copy ignite artifacts inside container
        3. Run ignite and write logs in main output /general.log
        4. Create process to write container logs in test directory
        5. Wait for topology changed

        :param host:                    target host
        :param node_config:             NodeConfig: node configuration
        :param wait_after:              Wait for topology after igntie start
        :param expected_servers_num:    Expected servers number in topology
        :param expected_clients_num:    Expected clients number in topology
        """
        node_id = node_config.node_order_number
        log_put(f'Starting node on {host}')
        self.nodes[node_id] = {
            'host': host,
            'status': NodeStatus.STARTING,
            'run_counter': 1
        }
        self.make_node_log(node_id)
        if node_config.network == 'host':
            swarm = False
            ekw_params, params = node_config.get_container_limits()
            con_id, log_path, con_name = self.docker.run(
                image_name="{}:{}".format(self.image_name, self.image_tag),
                host=host,
                network=node_config.network,
                name=f'ignite-{node_config.node_order_number}',
                commands=['touch', '/general.log', '&&', 'tail', '-f', '/general.log'],
                ekw_params=ekw_params,
                params=params,
                log_file=self.nodes[node_id].get('log')
            )
        else:
            swarm = True
            kw_params = node_config.get_service_limits()
            con_id, log_path, con_name = self.docker.create_service(
                host=host,
                image_name='{}:{}'.format(self.image_name, self.image_tag),
                network=node_config.network,
                name=f'ignite-{node_id}',
                commands=['touch', '/general.log', '&&', 'tail', '-f', '/general.log'],
                kw_params=kw_params
            )

        for artifact_type, container_path in [('ignite', '/ignite'), ('piclient', '/ignite/libs')]:
            remote_path = [art for art in self.config['artifacts'].values() if art.get('type') == artifact_type]
            tiden_assert(len(remote_path) > 0, 'Failed to find artifact with type {}'.format(artifact_type))
            remote_path = remote_path[0]['remote_path']
            self.docker.container_put(host=host, container_name=con_name, host_path=remote_path,
                                      container_path=container_path)

        self.docker.exec_in_container(cmd='mkdir /ignite/custom_config', container=con_name, host=host)

        main_config = ''
        for config_path in node_config.configs:
            if 'server' in config_path:
                main_config = basename(config_path)
            self.docker.container_put(host=host,
                                      container_name=con_name,
                                      host_path=config_path,
                                      container_path=f'/ignite/custom_config/{basename(config_path)}')
        piclients = [art for art in self.config['artifacts'].values() if art.get('type') == 'piclient']
        assert piclients, 'Failed to find piclient'

        self.docker.container_put(host, con_name, piclients[0]['remote_path'], '/ignite/libs/')

        ignite_args = self._get_jvm_options() + [
            '-J-DNODE_IP={}'.format(host),
            '-J-DNODE_COMMUNICATION_PORT={}'.format(self.get_node_communication_port(node_id)),
            '-J-DCONSISTENT_ID={}'.format(self.get_node_consistent_id(node_id)),
        ]

        start_command = 'nohup /ignite/bin/ignite.sh /ignite/custom_config/{} {} >> /general.log 2>&1 &'.format(
            main_config,
            ' '.join(ignite_args)
        )

        self.docker.exec_in_container(cmd='chmod -R a+x /ignite/bin/', container=con_name, host=host)
        self.docker.exec_in_container(cmd=start_command, container=con_name, host=host)

        self.nodes[node_id].update({
            'container_id': con_id,
            'container_name': con_name,
            'start_command': start_command,
            'swarm': swarm,
            'status': NodeStatus.STARTED,
            'ignite_home': '/ignite'
        })

        self.update_starting_node_attrs()

        if wait_after:
            if expected_servers_num is None:
                expected_servers_num = len(self.nodes)
            self.wait_for_topology_snapshot(server_num=expected_servers_num,
                                            client_num=expected_clients_num,
                                            timeout=60)

    def log_ignite_output(self, node_id):

        if node_id in self.nodes:
            self.docker.log_container_output(self.nodes[node_id]['host'],
                                             self.nodes[node_id]['container_id'],
                                             self.nodes[node_id]['log'])

    def activate_in_docker(self, node_id, user=None, password=None, swarm=False):
        """
        Activate cluster through one specific node

        :param node_id:     started node id
        :param user:        user name
        :param password:    user password
        """
        target_host = self.nodes[node_id]['host']
        host = self.nodes[node_id]['host']
        if swarm:
            host = 'tasks.ignite-{}'.format(node_id)
        con_name = self.nodes[node_id]['container_id']
        auth = ''
        if user is not None or password is not None:
            auth = '--user {} --password {}'.format(user, password)
        cmd = '{}/ignite/bin/control.sh --host {} --activate {}'.format(self.ignite_home, host, auth)
        test_dir = self.config["rt"]["remote"]["test_dir"]
        log_path = f'{test_dir}/activate_{node_id}.log'
        self.docker.exec_in_container(cmd, con_name, target_host, log_path)
        result = self.wait_for(lambda logs: [line for line in logs if 'Cluster activated' in line],
                               lambda: self.ssh.exec_on_host(target_host, ['cat {}'.format(log_path)])[target_host],
                               failed=lambda logs: [line for line in logs if 'failed' in line.lower()])
        tiden_assert(result, 'Failed to activate cluster')

    def start_node_inside(self, node_id):
        """
        Start java process inside container
        """
        host = self.nodes[node_id]['host']
        name = self.nodes[node_id]['container_name']
        container_id = self.nodes[node_id]['container_id']
        self.docker.start(host=host, name=container_id)
        self.wait_for(
            condition=lambda containers: [container for container in containers[host]
                                          if container['name'] == name and 'up' in container['status'].lower()],
            action=lambda: self.docker.get_running_containers()
        )
        self.nodes[node_id]['run_counter'] += 1
        self.make_node_log(node_id)
        self.docker.exec_in_container(self.nodes[node_id]['start_command'], container_id, host=host)
        self.nodes[node_id]['status'] = NodeStatus.STARTED
        self.log_ignite_output(node_id)

    def node_stop(self, node_id):
        super().node_stop(node_id)
        self.nodes[node_id]['status'] = NodeStatus.KILLED

    def clean_all_nodes(self):
        for node in self.nodes:
            self.docker.remove_containers(host=node['host'], name=node['container_name'])

    def clean_all_hosts(self):
        self.docker.leave_swarm()
        for host in self.ssh.hosts:
            log_put('Clean container on {}'.format(host))
            self.docker.remove_containers(host=host, name_pattern='ignite-.*')

    def wait_for_topology_snapshot(self, server_num=None, client_num=None, comment='', **kwargs):
        for node_id in self.nodes.keys():
            self.wait_for_topology_snapshot_on_node(
                node_id,
                server_num,
                client_num,
                timeout=kwargs.get('timeout', 30),
                interval=1
            )

    def wait_for_topology_snapshot_on_node(self, node_id, server_num=None, client_num=None, timeout=30, interval=1):
        end_time = time() + timeout
        host = self.nodes[node_id]['host']
        log = self.nodes[node_id]['log']
        name = self.nodes[node_id]['container_name']

        log_print(f'Waiting for topology snapshot on {host} in {name}. servers={server_num} clients={client_num}', color='green')
        while True:
            cmd = f'cat {log} | grep -E ".+Topology snapshot" | tail -n 1000'
            out = self.ssh.exec_on_host(host, [cmd])

            if out[host]:
                last_line = out[host][len(out[host]) - 1]
                match = search('\[(ver)=(\d+),.*(servers)=(\d+),.*(clients)=(\d+).+\]', last_line)

                if match:
                    found = {}
                    groups = match.groups()
                    for idx, item in enumerate(groups[::2]):
                        found[item] = groups[idx * 2 + 1]
                    result = True
                    if server_num is not None:
                        result += found['servers'] == server_num
                    if client_num is not None:
                        result += found['clients'] == server_num

                    print_found = 'Found topology {}'. \
                        format(' | '.join([f'{k}={v}' for k, v in found.items()]))
                    if result:
                        log_put(f'{print_found} success')
                        log_print()
                        return found
                    else:
                        log_put(f'{print_found} expected: servers={server_num} | clients={client_num}')
                        break
                else:
                    log_put('Waiting for topology'.format(log))

            if time() > end_time:
                tiden_assert(False, 'Failed to wait topology snapshot on {} for {} in {}'.format(host, name, log))

            sleep(interval)

    def _get_jvm_options(self):
        if self.config['environment'].get('server_jvm_options'):
            return ['-v'] + ['-J{}'.format(opt) for opt in self.config['environment'].get('server_jvm_options')]
        else:
            return ['-v', '-J-ea', '-J-Xmx2g', '-J-Xms2g']

    def exec_ignite_in_container(self, host, cmd):

        full_output = {}
        node_id = self._get_node(host, None)
        output = self.process_in_container(node_id, cmd)

        full_output[host] = output

        log_print(full_output, color='blue')
        return full_output

    def exec_ignite_in_containers(self, cmd):
        full_output = {}

        for host, orig_cmd in cmd.items():
            port = None
            if 'port' in orig_cmd:
                port = orig_cmd.split("port")[1].split()[0].strip()

            node_id = self._get_node(host, port)
            output = self.process_in_container(node_id, orig_cmd)

            full_output[host] = output

        log_print(full_output, color='blue')
        return full_output

    def process_in_container(self, node_id, orig_cmd):
        new_cmd = None
        log_file, log_in_container = None, None
        output = []
        target_host = self.nodes[node_id]['host']

        con_name = self.nodes[node_id]['container_id']

        if not isinstance(orig_cmd, list):
            orig_cmd = [orig_cmd]

        for cmd in orig_cmd:
            updated_cmd = cmd

            # change home to one in the container
            if 'cd ' in updated_cmd:
                new_cmd = updated_cmd.split()
                path_position = new_cmd.index('cd') + 1
                new_cmd.pop(path_position)
                new_cmd.insert(path_position, '/ignite;')

            # change log file to one in the container
            if '>' in updated_cmd:
                if not new_cmd:
                    new_cmd = updated_cmd.split()
                log_position = new_cmd.index('>') + 1
                log_file = new_cmd.pop(log_position)
                log_in_container = '/{}'.format(log_file.split('/')[-1])
                new_cmd.insert(log_position, log_in_container)

            if new_cmd:
                updated_cmd = ' '.join(new_cmd)

            tmp_out = self.docker.exec_in_container(updated_cmd, con_name, target_host)
            # if tmp_out:
            output.append(tmp_out)

            if log_file:
                self.docker.container_get(target_host, con_name, log_in_container, log_file)

        return output

    def _get_node(self, host, port):
        found_node_id = None

        for node_id, node_params in self.nodes.items():
            if host and node_params.get('host') == host:
                found_node_id = node_id
                if port and node_params.get('binary_rest_port') == port:
                    return node_id

        return found_node_id

