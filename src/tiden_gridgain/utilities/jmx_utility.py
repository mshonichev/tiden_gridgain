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

import re

from py4j.java_gateway import JavaGateway, GatewayParameters
from py4j.protocol import Py4JJavaError

from tiden import log_print, time, sleep, TidenException
from ..piclient.piclient import PiClientType, PiClientStarter, PiClient


class JmxUtility:
    # seconds to sleep after next collecting try
    rebalance_collect_timeout = 1
    rebalance_before_sleep = 5

    def __init__(self, ignite=None, start_gateway=True):
        """
        Initialize JMX Utility based on piclient.jar

        If ignite instance is defined - then utility will be started as corresponding ignite.app node
        Otherwise it will not start anything. User can pass gateway, service and nodes manually using initialize()

        :param ignite: tiden.app.ignite instance
        :param start_gateway: do create gateway instance - oy4j issue with ProcessPoolExecutor workaround
        """
        self.start_gateway = start_gateway

        self._started = False
        self._proxy = False
        self.nodes = None

        if ignite:
            self.ignite = ignite
            self.starter = PiClientStarter(ignite, start_gateway=start_gateway)
        else:
            self.node_id = None
            self.gateway = None
            self.service = None

    def start_utility(self):
        self._started = True

        self.node_id = self.starter.start_common_nodes(num=1, piclient_type=PiClientType.JMX)[0]

        # todo wat
        self.nodes = self.ignite.nodes
        if self.start_gateway:
            self.gateway = self.nodes[self.node_id]['gateway']
            self.service = self.nodes[self.node_id]['gateway'].entry_point.getJmxService()
        return self.node_id

    def kill_utility(self):
        assert self._started, 'Trying to kill utility that already dead or initialized manually.'

        self.starter.shutdown(self.node_id, start_gateway=self.start_gateway)
        log_print("JMX utility killed")
        self._started = False

    def initialize_manually(self, jmx_node_id, ignite_nodes):
        """
        Initialize manually. It may be useful when JMX should be started in separate process:
        There is no way to pass object in process, but you able to pass jmx_node_id and use it to connect gateway

        kill_manually() method should be used in case of manually started jmx utility

        :param jmx_node_id: ignite_nodes[jmx_node_id] should contain gateway_port and host keys
        :param ignite_nodes: ignite.app
        :return: None
        """
        log_print("Initializing JMX Utility manually")
        self._proxy = True

        self.nodes = ignite_nodes
        self.gateway = JavaGateway(
            gateway_parameters=GatewayParameters(address=ignite_nodes[jmx_node_id]['host'],
                                                 port=int(ignite_nodes[jmx_node_id]['gateway_port']),
                                                 read_timeout=PiClient.read_timeout))
        self.service = self.gateway.entry_point.getJmxService()

    def kill_manually(self):
        """
        Manually kill utility: close gateway and remove variables from VM
        In case of using other ways to kill utility errors may appear in tiden.log

        NB! Does not remove node from self.ignite cluster. Should be called after kill_manually()
        """
        assert self._proxy, 'Trying to kill utility that was NOT created manually'

        log_print("Killing JMX Utility manually")
        self.gateway.shutdown()

        del self.gateway
        del self.service

    def get_attributes(self, node_id, grp, bean, *attributes, prefix=None, cls_ldr=None, instance=None):
        """
        Get attribute values

        :param node_id: ignite node id to take host and jmx port
        :param attributes: list of attributes to take
        :param grp: optional groupName
        :param bean: optional bean name
        :param prefix: optional prefix (if defined for ignite)
        :param cls_ldr: optional classLoader (if defined for ignite)
        :param instance: optional instance (if defined for ignite)
        :return: dict(str: str) attribute_name to value
        """
        if len(attributes) == 0:
            return

        self.assert_started()

        host, port = self._get_jmx_host_port(node_id)

        string_class = self.gateway.jvm.java.lang.String
        attr_array = self.gateway.new_array(string_class, len(attributes))

        for i, arg in enumerate(attributes):
            attr_array[i] = arg

        result = self.service.getAttributes(
            host, str(port),
            prefix, cls_ldr, instance,
            grp,
            bean,
            attr_array
        ).split('\n')

        return dict((k.strip(), v.strip()) for k, v in
                    (item.split(': ', 1) for item in result if item))

    def evaluate_operation(self, node_id, grp, bean, operation, *args, prefix=None, cls_ldr=None, instance=None):
        """
        Evaluate JMX operation

        Note that args should be presented in java-compatible format:
        boolean values should be string 'true'/'false'

        :param node_id: ignite node id to take host and jmx port
        :param operation: operation name
        :param args: operation args
        :param grp: optional groupName
        :param bean: optional bean name
        :param prefix: optional prefix (if defined for ignite)
        :param cls_ldr: optional classLoader (if defined for ignite)
        :param instance: optional instance (if defined for ignite)
        :return: operation result (usually just 'null')
        """
        self.assert_started()

        arguments = '(%s)' % ','.join(args)

        host, port = self._get_jmx_host_port(node_id)

        string_class = self.gateway.jvm.java.lang.String
        operation_array = self.gateway.new_array(string_class, 1)

        operation_array[0] = str(operation) + arguments

        return self.service.evaluateOperations(host, str(port), prefix, cls_ldr, instance, grp, bean, operation_array)

    def list(self, node_id, grp=None, bean=None, prefix=None, cls_ldr=None, instance=None, output_format='yaml'):
        """
        Return list of attributes, operation etc. for defined host,port in specified format
        If group and bean name defined - attributes and operations will be taken from specified object address

        :param node_id: ignite node id to take host and jmx port
        :param grp: optional groupName
        :param bean: optional bean name
        :param prefix: optional prefix (if defined for ignite)
        :param cls_ldr: optional classLoader (if defined for ignite)
        :param instance: optional instance (if defined for ignite)
        :param output_format: json,yaml,plain
        :return: string in defined output format
        """
        self.assert_started()

        host, port = self._get_jmx_host_port(node_id)

        if not grp or not bean:
            return self.service.getAllList(host, str(port), output_format)
        else:
            return self.service.getList(host, str(port), prefix, cls_ldr, instance, grp, bean, output_format)

    def _get_jmx_host_port(self, node_id):
        host = self.nodes[node_id]['host']
        port = self.nodes[node_id]['jmx_port']

        if host is None or port is None:
            raise TidenException("Either host or port is not defined (host=%s, port=%s)" % (host, port))

        return host, port

    def activate(self, node_id):
        self.assert_started()

        grp, bean, operation, args = 'Kernal', 'IgniteKernal', 'active', 'true'
        return self.evaluate_operation(node_id, grp, bean, operation, args)

    def deactivate(self, node_id):
        self.assert_started()

        grp, bean, operation, args = 'Kernal', 'IgniteKernal', 'active', 'false'
        return self.evaluate_operation(node_id, grp, bean, operation, args)

    def wait_for_finish_rebalance(self, rebalance_timeout, cache_groups, calculate_exact_time=False, time_for_node=None,
                                  log=True):
        """
        Wait for finish rebalance using JMX metric Cache groups/$CACHE_GROUP_NAME/LocalNodeMovingPartitionsCount

        :param rebalance_timeout: maximum timeout
        :param cache_groups: cache groups (may be collected using collect_cache_group_names() method)
        :param calculate_exact_time: calculate exact time from logs
        :param time_for_node: get time from node to rebalance
        :param log: do log "Current metric state" and "Wait rebalance" log entries
        :return: time for rebalance completed
        """
        timeout_counter = 0
        rebalance_finished = False
        started = int(time())

        sleep(JmxUtility.rebalance_before_sleep)

        while timeout_counter < rebalance_timeout and not rebalance_finished:

            sleep(JmxUtility.rebalance_collect_timeout)

            rebalance_finished = True

            try:
                for node in self.ignite.get_alive_default_nodes():
                    for cache_name in cache_groups:
                        moving_partitions = \
                            self.get_attributes(node,
                                                "Cache groups",
                                                cache_name,
                                                "LocalNodeMovingPartitionsCount"
                                                )['LocalNodeMovingPartitionsCount']

                        if int(moving_partitions) != 0:
                            if log:
                                log_print("Current metric state for cache %s on node %s: %s" %
                                          (cache_name, node, moving_partitions), color='yellow')
                            rebalance_finished = False
                            break

                    if not rebalance_finished:
                        break
            except Py4JJavaError:
                # log_print("Failed to get attributes: {}".format(traceback.format_exc()), color='red')
                sleep(JmxUtility.rebalance_collect_timeout)
                continue

            timeout_counter = int(time()) - started

            if rebalance_finished:
                if log:
                    log_print("Rebalance finished in {} seconds".format(timeout_counter))

                break

            if log:
                log_print("Wait rebalance to finish {}/{}".format(timeout_counter, rebalance_timeout))

        if not rebalance_finished:
            raise AssertionError("Failed to wait rebalance completed")

        if calculate_exact_time:
            return self.calculate_exact_time(cache_groups, time_for_node)

        if log:
            log_print()

        return timeout_counter

    def dr_status(self, cache_name, node_id=None):
        self.assert_started()

        if not node_id:
            nodes = self.ignite.get_alive_default_nodes()
            node_id = nodes[-1]
        grp, bean, operation = cache_name, 'Cache data replication', 'DrStatus'
        return self.get_attributes(node_id, grp, bean, operation)

    def dr_stop(self, cache_name, node_id=None):
        self.assert_started()

        if not node_id:
            nodes = self.ignite.get_alive_default_nodes()
            node_id = nodes[1]

        grp, bean, operation = cache_name, 'Cache data replication', 'stop'
        return self.evaluate_operation(node_id, grp, bean, operation)

    def dr_start(self, cache_name, node_id=None):
        self.assert_started()

        if not node_id:
            nodes = self.ignite.get_alive_default_nodes()
            node_id = nodes[1]

        grp, bean, operation = cache_name, 'Cache data replication', 'start'
        return self.evaluate_operation(node_id, grp, bean, operation)

    def dr_pause(self, cache_name, node_id=None):
        self.assert_started()

        if not node_id:
            nodes = self.ignite.get_alive_default_nodes()
            node_id = nodes[-1]

        grp, bean, operation = cache_name, 'Cache data replication', 'pause'
        return self.evaluate_operation(node_id, grp, bean, operation)

    def dr_resume(self, cache_name, node_id=None):
        self.assert_started()

        if not node_id:
            nodes = self.ignite.get_alive_default_nodes()
            node_id = nodes[-1]

        grp, bean, operation = cache_name, 'Cache data replication', 'resume'
        return self.evaluate_operation(node_id, grp, bean, operation)

    def dr_state_transfer(self, cache_name, node_id=None, dcr_id=2):
        self.assert_started()

        if not node_id:
            nodes = self.ignite.get_alive_default_nodes()
            node_id = nodes[1]

        grp, bean, operation, args = cache_name, 'Cache data replication', 'transferTo', str(dcr_id)
        return self.evaluate_operation(node_id, grp, bean, operation, args)

    def dr_pause_for_all_receivers(self, node_id=None):
        self.assert_started()

        grp, bean, operation = 'Data center replication', 'Sender hub', 'pause'
        return self.evaluate_operation(node_id, grp, bean, operation)

    def dr_resume_for_all_receivers(self, node_id=None):
        self.assert_started()

        grp, bean, operation = 'Data center replication', 'Sender hub', 'resume'
        return self.evaluate_operation(node_id, grp, bean, operation)

    def dr_get_status(self, cache_groups, metric_name, log=True):
        """
        Wait for finish rebalance using JMX metric Cache groups/$CACHE_GROUP_NAME/LocalNodeMovingPartitionsCount

        :param rebalance_timeout: maximum timeout
        :param cache_groups: cache groups (may be collected using collect_cache_group_names() method)
        :param log: do log "Current metric state" and "Wait rebalance" log entries
        :return: time for rebalance completed
        """
        value = None
        try:
            for node in self.ignite.get_alive_default_nodes():
                if not isinstance(cache_groups, list):
                    cache_groups = [cache_groups]

                for cache_name in cache_groups:
                    try:
                        value = \
                            self.get_attributes(node,
                                                cache_name,
                                                "Cache data replication",
                                                metric_name
                                                )[metric_name]
                    except Exception as e:
                        log_print('Exception on {} {}'.format(node, cache_name))
                        log_print(e)

        except Py4JJavaError:
            log_print("Failed to get attributes: {}".format(traceback.format_exc()), color='red')
            sleep(JmxUtility.rebalance_collect_timeout)

        if log:
            log_print("Found value {} for cache {} metric name ()".
                      format(value, cache_groups, metric_name))

        return value

    def wait_for_dr_attributes(self, rebalance_timeout, cache_groups, metric_name='DrBatchWaitingSendCount',
                               expected_value=0, log=True):
        """
        Wait for finish rebalance using JMX metric Cache groups/$CACHE_GROUP_NAME/LocalNodeMovingPartitionsCount

        :param rebalance_timeout: maximum timeout
        :param cache_groups: cache groups (may be collected using collect_cache_group_names() method)
        :param log: do log "Current metric state" and "Wait rebalance" log entries
        :return: time for rebalance completed
        """
        timeout_counter = 0
        rebalance_finished = False
        started = int(time())

        while timeout_counter < rebalance_timeout and not rebalance_finished:

            # sleep(JmxUtility.rebalance_collect_timeout)

            value_found = True
            current_value = None
            try:
                for node in self.ignite.get_alive_default_nodes():
                    if not isinstance(cache_groups, list):
                        cache_groups = [cache_groups]

                    for cache_name in cache_groups:
                        try:
                            current_value = \
                                self.get_attributes(node,
                                                    cache_name,
                                                    "Cache data replication",
                                                    metric_name
                                                    )[metric_name]
                        except Exception as e:
                            log_print('Exception on {} {}'.format(node, cache_name))
                            log_print(e)

                        convert_to = str
                        if isinstance(expected_value, int):
                            convert_to = int

                        if convert_to(current_value) != expected_value:
                            if log:
                                log_print("Current value for node: {} cache {} [{}] value: {}. Waiting for value {}"
                                          .format(node, cache_name, metric_name, current_value, expected_value),
                                          color='yellow')
                                value_found = False
                            break

                    if not value_found:
                        break
            except Py4JJavaError:
                log_print("Failed to get attributes: {}".format(traceback.format_exc()), color='red')
                sleep(JmxUtility.rebalance_collect_timeout)
                continue

            timeout_counter = int(time()) - started

            if value_found:
                if log:
                    log_print("Value {} for cache {} found in {} seconds".
                              format(expected_value, cache_groups, timeout_counter))

                break

            if log:
                log_print("Waiting for value {}/{}".format(timeout_counter, rebalance_timeout))

        if not value_found:
            raise AssertionError("Failed to wait replication completed")

        if log:
            log_print()

        return timeout_counter

    def calculate_exact_time(self, cache_groups, time_for_node):
        """
        Calculate exact rebalance time from Ignite logs

        :param cache_groups: cache groups to check
        :param time_for_node: do check time on a specific node (not on whole cluster)
        :return: rebalance time
        """
        rebalance_start_time = 0
        rebalance_end_time = 0
        for node in self.ignite.get_alive_default_nodes():
            # if time_for_node defined - rebalance time will be calculated on single node
            # otherwise on whole cluster
            if time_for_node and node != time_for_node:
                continue

            for cache_name in cache_groups:
                host = self.nodes[node]['host']
                log_print("Collect timing for node %s" % node)

                # get Prepared rebalancing log message
                cmd = 'cat %s | grep -E "Prepared rebalancing \[grp=%s" | tail -1' % (
                    self.nodes[node]['log'], cache_name
                )
                rebalance_start_time = self.get_tms_from_log(self.get_log_lines(host, cmd),
                                                             rebalance_start_time)

                # get completed rebalance future
                cmd = 'cat %s | grep -E "Completed rebalance future: ' \
                      'RebalanceFuture \[grp=CacheGroupContext \[grp=%s\]," | tail -1' % (
                          self.nodes[node]['log'], cache_name
                      )
                rebalance_end_time = self.get_tms_from_log(self.get_log_lines(host, cmd),
                                                           rebalance_end_time)

        return (rebalance_end_time - rebalance_start_time) / 1000

    def get_log_lines(self, host, cmd):
        commands = {host: [cmd, ]}
        res = self.ignite.ssh.exec(commands)
        lines = str(res[host][0]).split('\n')

        return lines

    def get_tms_from_log(self, lines, latest_collected_time):
        m = re.search('\[tms-(\d+)\]', lines[0])
        if m:
            for group in m.groups():
                int_value = int(group)
                if not latest_collected_time or latest_collected_time < int_value:
                    latest_collected_time = int_value
                break

        if not latest_collected_time:
            log_print("Looks that there is no [tms-UNIX_TIME_MILLIS] in log.Check logger.", color='red')

        return latest_collected_time

    def is_started(self):
        return self._started

    def is_proxy(self):
        return self._proxy

    def assert_started(self):
        assert self._started or self._proxy, "JmxUtility must be started or initialized manually"

