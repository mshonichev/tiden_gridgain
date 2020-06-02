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

from .multigridtestcase import MultiGridTestCase
from tiden.util import *
from time import sleep


class YardstickTestCase (MultiGridTestCase):

    throughput_latency_probe_csv_name = 'ThroughputLatencyProbe.csv'
    driver_heap_size = 6

    def setup(self):
        super().setup()

    def start_benchmark_drivers(self,
                                drivers_count,
                                ignite_name,
                                benchmark_class,
                                benchmark_name,
                                attempt,
                                jvm_opts,
                                driver_options):
        driver_options_str = ''
        for opt_name in driver_options.keys():
            driver_options_str += "%s %s " % (opt_name, driver_options[opt_name])
        write_sync_mode = driver_options['-sm']
        driver_options_str = driver_options_str[:-1]
        # Client work directory
        remote_home = self.config['rt']['remote']['test_dir']
        class_paths = []
        # Construct class path
        for lib_dir in ['libs', 'libs/ignite-spring', 'libs/ignite-indexing', 'benchmarks/libs']:
            class_paths.append("%s/%s/*" % (self.ignite[ignite_name].client_ignite_home, lib_dir))
        method_home = self.config['rt']['remote']['test_dir']
        module_home = self.config['rt']['remote']['test_module_dir']
        cmd_args = "%s  " \
                   "-cfg %s/%s " \
                   "-dn %s " \
                   "-ds %s " \
                   "--config %s/benchmark.properties " \
                   "--logsFolder %s " \
                   "--currentFolder %s " \
                   "--scriptsFolder %s/bin" % \
                   (
                       driver_options_str,
                       module_home,
                       self.get_client_config(),
                       benchmark_class,
                       benchmark_name,
                       module_home,
                       method_home,
                       method_home,
                       method_home
                   )
        clients = {}
        jvm_opts_str = ' '.join(jvm_opts)
        if self.config['environment'].get('client_jvm_options'):
            jvm_opts_str += " %s" % ' '.join(self.config['environment']['client_jvm_options'])
        log_print("Yardstick drivers jvm options: %s" % jvm_opts_str)
        log_print("Yardstick drivers benchmark arguments: %s" % cmd_args)
        log_print("Yardstick benchmark, %s driver(s) starting" % drivers_count)
        # env_vars = ''
        # if self.config['environment'].get('env_vars'):
        #     for env_var_name in self.config['environment']['env_vars'].keys():
        #         env_vars += "%s=%s;" % (env_var_name, self.config['environment']['env_vars'][env_var_name])
        for client in range(0, len(self.config['environment']['client_hosts'])):
            # Get next client host
            host = self.config['environment']['client_hosts'][client]
            # Find client node index
            node_index = 50000 + 1000*(client+1) + attempt
            while self.ignite[ignite_name].nodes.get(node_index) is not None and node_index < 1999:
                node_index += 1
            # Client command line
            cmd = "cd %s; " \
                  "nohup " \
                  "  $JAVA_HOME/bin/java " \
                  "    -cp %s %s " \
                  "    -DCONSISTENT_ID=%s " \
                  "    -DIGNITE_QUIET=false " \
                  "  org.yardstickframework.BenchmarkDriverStartUp " \
                  "    --outputFolder %s/%s.%s %s" \
                  "> %s/grid.%s.node.%s.0.log 2>&1 &" \
                  % (
                      self.ignite[ignite_name].client_ignite_home,
                      # env_vars,
                      ':'.join(class_paths),
                      jvm_opts_str,
                      self.ignite[ignite_name].get_node_consistent_id(node_index),
                      method_home,
                      ignite_name,
                      (client+1),
                      cmd_args,
                      remote_home,
                      self.ignite[ignite_name].grid_name,
                      str(node_index)
                  )
            self.ignite[ignite_name].nodes[node_index] = {
                'host': host,
                'log': remote_home + '/grid.' +
                   str(self.ignite[ignite_name].grid_name) + '.node.' + str(node_index) + '.0.log',
                'run_counter': 0
            }
            if not clients.get(host):
                clients[host] = [cmd]
            else:
                clients[host].append(cmd)
        self.ssh.exec(clients)
        self.ignite[ignite_name].wait_for_topology_snapshot(
            self.ignite[ignite_name].get_nodes_num('server'),
            drivers_count,
            ''
        )
        log_print("Yardstick benchmark, %s driver(s) started" % drivers_count)

    def collect(self):
        results = {}
        for ignite_name in self.ignite.keys():
            results[ignite_name] = {}
            result_root_dir = "%s/%s" % (self.config['rt']['remote']['test_dir'], ignite_name)
            # Cat csv files
            data = self.ssh.exec(['cat %s.*/*/%s' % (result_root_dir, self.throughput_latency_probe_csv_name)])
            cur = {}
            raw_csv = {}
            for host in data.keys():
                raw = data[host]
                if not cur.get(host):
                    cur[host] = {
                        'throughput_sum': 0,
                        'throughput_avg': 0,
                        'latency_sum': 0,
                        'latency_avg': 0,
                        'count': 0,
                    }
                    raw_csv[host] = []
                # Parse the results and summarize them
                for line in raw[0].split("\n"):
                    raw_csv[host].append(line)
                    m = search('^(\d+),([0-9\.]+),([0-9\.]+)$', line)
                    if m:
                        cur[host]['throughput_sum'] += float(m.group(2))
                        cur[host]['latency_sum'] += float(m.group(3))
                        cur[host]['count'] += 1
            if len(cur) > 0:
                total_throughput = 0
                total_latency = 0
                count = 0
                for host in cur.keys():
                    if cur[host]['count'] > 0:
                        cur[host]['throughput_avg'] = cur[host]['throughput_sum']/cur[host]['count']
                        cur[host]['latency_avg'] = cur[host]['latency_sum']/cur[host]['count']
                        total_throughput += cur[host]['throughput_avg']
                        total_latency += cur[host]['latency_avg']
                        count += 1
                results[ignite_name] = {
                    'total_throughput': total_throughput,
                    'client_throughput': total_throughput/count,
                    'client_latency': total_latency/count,
                    self.throughput_latency_probe_csv_name.replace('.csv', ''): raw_csv
                }
                res_str = ''
                for res_name in results[ignite_name].keys():
                    if isinstance(results[ignite_name][res_name], dict):
                        continue
                    res_str += "%s: %.2f, " % (res_name, results[ignite_name][res_name])
                res_str = res_str[:-2]
                log_print("%s results: %s" % (ignite_name, res_str))
        return results

    def collect_per_run(self):
        results = {}
        for ignite_name in self.ignite.keys():
            results[ignite_name] = {}
            result_root_dir = "%s/%s" % (self.config['rt']['remote']['test_dir'], ignite_name)
            dir_data = self.ssh.exec(['ls -d %s.*/*' % result_root_dir])
            result_dirs = set()
            result_hosts = set()
            for host in dir_data.keys():
                lines = dir_data[host][0].split('\n')
                for line in lines:
                    if line.startswith(result_root_dir):
                        result_dirs.add(line)
                        result_hosts.add(host)
            cur = {}
            raw_csv = {}
            for result_dir in result_dirs:
                results[ignite_name][result_dir] = {}
                # Cat csv files
                commands = {}
                for host in result_hosts:
                    commands[host] = ['cat %s/%s' % (result_dir, self.throughput_latency_probe_csv_name)]
                data = self.ssh.exec(commands)
                cur[result_dir] = {}
                raw_csv[result_dir] = {}
                for host in data.keys():
                    raw = data[host]
                    if not cur.get(host):
                        cur[result_dir][host] = {
                            'throughput_sum': 0,
                            'throughput_avg': 0,
                            'latency_sum': 0,
                            'latency_avg': 0,
                            'count': 0,
                        }
                        raw_csv[result_dir][host] = []
                    # Parse the results and summarize them
                    for line in raw[0].split("\n"):
                        raw_csv[result_dir][host].append(line)
                        m = search('^(\d+),([0-9\.]+),([0-9\.]+)$', line)
                        if m:
                            cur[result_dir][host]['throughput_sum'] += float(m.group(2))
                            cur[result_dir][host]['latency_sum'] += float(m.group(3))
                            cur[result_dir][host]['count'] += 1
                if len(cur[result_dir]) > 0:
                    total_throughput = 0
                    total_latency = 0
                    count = 0
                    for host in cur[result_dir].keys():
                        if cur[result_dir][host]['count'] > 0:
                            cur[result_dir][host]['throughput_avg'] \
                                = cur[result_dir][host]['throughput_sum']/cur[result_dir][host]['count']
                            cur[result_dir][host]['latency_avg'] \
                                = cur[result_dir][host]['latency_sum']/cur[result_dir][host]['count']
                            total_throughput += cur[result_dir][host]['throughput_avg']
                            total_latency += cur[result_dir][host]['latency_avg']
                            count += 1
                    results[ignite_name][result_dir] = {
                        'total_throughput': total_throughput,
                        'client_throughput': total_throughput/count,
                        'client_latency': total_latency/count,
                        self.throughput_latency_probe_csv_name.replace('.csv', ''): raw_csv
                    }
                    # res_str = ''
                    # for res_name in results[ignite_name].keys():
                    #     if isinstance(results[ignite_name][res_name], dict):
                    #         continue
                    #     res_str += "%s: %.2f, " % (res_name, results[ignite_name][res_name])
                    #res_str = res_str[:-2]
                    #log_print("%s results: %s" % (ignite_name, res_str))
        return results

    def wait_for_drivers(self, driver_limit, timeout):
        """
        Wait for finished drivers
        :param      driver_limit:   the number of running drivers
        :param      timeout:        timeout (sec)
        :return:    True if the running drivers matched drivers limit, otherwise False
        """
        started = int(time())
        timeout_counter = 0
        res = False
        while (int(time()) - started) < timeout:
            processes = self.ssh.jps()
            driver_count = 0
            for proc_data in processes:
                if 'BenchmarkDriverStartUp' in proc_data['name']:
                    driver_count += 1
            log_put(
                "Waiting for running drivers %s/%s, %s/%s sec " %
                (
                    driver_count,
                    driver_limit,
                    (int(time()) - started),
                    timeout
                )
            )
            stdout.flush()
            if driver_limit == driver_count:
                res = True
                break
            sleep(10)
        log_print()
        return res

