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

from tiden.ignite import Ignite
from tiden.case.generaltestcase import GeneralTestCase
from tiden.util import *
from tiden.utilities.control_utility import ControlUtility
from tiden_gridgain.utilities.snapshot_utility import SnapshotUtility
from tiden.tidenexception import *
from re import search
from time import sleep, time


class MultiGridTestCase(GeneralTestCase):
    """
    The base class for tests with single grid.
    The main purpose is start grid on hosts
    """

    preloading_size = 120000

    def __init__(self, config, ssh):
        self.ignite = {}
        self.cu = {}
        self.su = {}
        super().__init__(config, ssh)
        for artifact_name in config.get('artifacts', {}).keys():
            if config['artifacts'][artifact_name].get('type'):
                if config['artifacts'][artifact_name]['type'] == 'ignite':
                    self.ignite[artifact_name] = Ignite(config, ssh, name=artifact_name)
                    self.cu[artifact_name] = ControlUtility(self.ignite[artifact_name])
                    self.su[artifact_name] = SnapshotUtility(self.ignite[artifact_name])
#        assert len(self.ignite) > 1, 'At least two ignite artifacts found'

    def setup(self):
        for name, test_context in self.contexts.items():
            test_context.set_client_result_config("client_%s.xml" % name)
            test_context.set_server_result_config("server_%s.xml" % name)

            test_context.build_config()

        super().deploy()
        for ignite in self.ignite.values():
            ignite.setup()

        # Fix shell scripts permissions
        for artifact_name in self.config.get('artifacts', {}).keys():
            if self.config['artifacts'][artifact_name].get('type'):
                if self.config['artifacts'][artifact_name]['type'] == 'ignite':
                    self.ssh.exec(
                        ["chmod -v 0755 %s/bin/*.sh" % self.config['artifacts'][artifact_name]['remote_path']]
                    )

    def set_current_context(self, name, context_name='default'):
        super().set_current_context(context_name)
        self.ignite[name].set_node_option('*', 'config', self.get_server_config())

    def start_grid(self, name):
        self.ignite[name].start_nodes()
        self.cu[name].activate()

    def stop_grid(self, name):
        self.cu[name].deactivate()
        sleep(5)
        self.ignite[name].stop_nodes()
        sleep(5)

    def restart_grid(self, name):
        self.cu[name].deactivate()
        sleep(5)
        self.ignite[name].stop_nodes()
        sleep(5)
        self.ignite[name].start_nodes()
        self.cu[name].activate()

    def restart_empty_grid(self, name):
        self.cu[name].deactivate()
        sleep(5)
        self.ignite[name].stop_nodes()
        sleep(5)
        self.delete_lfs(name)
        self.ignite[name].start_nodes()
        self.cu[name].activate()

    def log_cache_entries(self):
        cache_names = self.ignite.get_cache_names('cache_')
        entry_num = self.ignite.get_entries_num(cache_names)
        log_print("Found %s entries in %s cache(s)" % (entry_num, len(cache_names)), 2)

    def wait_for_entries_num(self, name, cache_name_prefix, size, timeout, cond='=='):
        """
        The method is waiting for total number of entries for all selected caches
        :param cache_name_prefix:   prefix of cache name
        :param size:                awaited size
        :param timeout:             timeout (in sec)
        :return:
        """
        cache_names = self.ignite[name].get_cache_names(cache_name_prefix)
        log_put("Waiting: %s caches found" % len(cache_names))
        # Main loop
        started = int(time())
        timeout_counter = 0
        current_size = 0
        matched = False
        while timeout_counter < timeout:
            current_size = self.ignite[name].get_entries_num(cache_names)
            log_put("Waiting in %s cache(s): entries number: %s/%s, timeout %s/%s sec"
                              % (len(cache_names),
                                 current_size,
                                 size,
                                 timeout_counter,
                                 timeout)
                    )
            if cond == '==':
                if current_size == size:
                    matched = True
                    break
            elif cond == '>=':
                if current_size >= size:
                    matched = True
                    break
            elif cond == '<=':
                if current_size <= size:
                    matched = True
                    break
            sleep(5)
            timeout_counter = int(time()) - started
        if not matched:
            raise TidenException('Waiting for entries number failed')
        else:
            log_print()

    def start_simple_clients(self, *args, **kwargs):
        name = args[0]
        ignite_config = args[1]
        glob_path = args[2]
        dump_suffix = None
        if len(args) == 4:
            dump_suffix = args[3]
        files = glob(glob_path)
        if len(files) > 0:
            for file in files:
                file_name = path.basename(file)
                client_args = [
                    '-threads=8',
                    "-config=%s" % ignite_config,
                    "-operation-file=%s/%s" % (self.config['rt']['remote']['test_module_dir'], file_name),
                    "-operation=sleep:10"
                ]
                message = "processing %s" % file_name
                if dump_suffix is not None:
                    client_args.append(
                        "-dump-file=%s.%s" % (file_name, dump_suffix)
                    )
                    message = "processing %s to %s.%s" % (file_name, file_name, dump_suffix)
                self.ignite[name].start_simple_client(
                    client_args,
                    message,
                    **kwargs
                )
        else:
            self.ignite[name].start_simple_client(
                [
                    '-threads=8',
                    "-config=%s" % ignite_config,
                    args[2],
                    "-operation=sleep:10"
                ],
                "processing %s" % args[2],
                **kwargs
            )
        if kwargs.get('waiting_timeout') is not None:
            self.wait_for_running_clients_num(name, 0, kwargs.get('waiting_timeout'))

    def wait_for_running_clients_num(self, name, client_num, timeout):
        """
        Waiting for given number of running client nodes
        :param client_num:  number of client nodes
        :param timeout:     timeout in seconds
        :return:
        """
        started = int(time())
        timeout_counter = 0
        current_num = 0
        while timeout_counter < timeout:
            current_num = self.ignite[name].get_nodes_num('client')
            if current_num == client_num:
                break
            log_put("Waiting running clients: %s/%s, timeout %s/%s sec"
                              % (current_num, client_num, timeout_counter, timeout))
            sleep(5)
            timeout_counter = int(time()) - started
        if current_num != client_num:
            raise TidenException('Waiting for client number failed')
        else:
            log_print("Found %s running client node(s) in %s/%s sec"
                              % (current_num, timeout_counter, timeout))

    def save_lfs(self, name, tag, dir_path=None):
        """
        Copy Ignite LFS
        :param      tag:        name of tag, used for filename of zip archive
        :param      dir_path:   remote path of LFS zip archive
        :return:    None
        """
        log_put("Storing Ignite LFS to '%s' ... " % tag)
        if dir_path is None:
            dir_path = self.config['remote']['suite_var_dir']
        commands = {}
        started = time()
        for node_idx in self.ignite[name].nodes.keys():
            if node_idx >= 1000:
                continue
            host = self.ignite[name].nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    # 'cd %s; zip --symlinks -r ignite_lfs_%s.%s.zip %s' % (
                    'cd %s; tar -cf ignite_lfs_%s.%s.tar %s' % (
                        self.config['rt']['remote']['test_module_dir'],
                        tag,
                        host,
                        '*server.*/work/db/* *server.*/work/binary_meta/* *server.*/work/marshaller/*',
                    ),
                    'mv %s/ignite_lfs_%s.%s.tar %s' % (
                        self.config['rt']['remote']['test_module_dir'],
                        tag,
                        host,
                        dir_path
                    ),
                    'stat -c "%' + 's" %s/ignite_lfs_%s.%s.tar' % (
                        dir_path,
                        tag,
                        host
                    ),
                ]
        results = self.ssh.exec(commands)
        total_size = 0
        for host in results.keys():
            lines = results[host][2]
            m = search('^([0-9]+)\n', lines)
            if m:
                total_size += int(m.group(1))
        log_put("Ignite LFS stored in '%s' in %s sec, size: %s bytes" % (
            tag, int(time()-started), "{:,}".format(total_size))
        )
        log_print()

    def restore_lfs(self, name, tag, dir_path=None):
        log_put("Restore Ignite LFS from '%s' ... " % tag)
        if dir_path is None:
            dir_path = self.config['remote']['suite_var_dir']
        commands = {}
        started = time()
        for node_idx in self.ignite[name].nodes.keys():
            if node_idx >= 1000:
                continue
            host = self.ignite[name].nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    'cp -f %s/ignite_lfs_%s.%s.tar %s' % (
                        dir_path,
                        tag,
                        host,
                        self.config['rt']['remote']['test_module_dir']
                    ),
                    'cd %s; tar -xf ignite_lfs_%s.%s.tar ' % (
                        self.config['rt']['remote']['test_module_dir'],
                        tag,
                        host,
                    )
                ]
        results = self.ssh.exec(commands)
        log_put("Ignite LFS restored from '%s' in %s sec" % (tag, int(time()-started)))
        log_print()

    def delete_lfs(self, name):
        log_put("Delete Ignite LFS for ignite %s ... " % name)
        commands = {}
        started = time()
        for node_idx in self.ignite[name].nodes.keys():
            if node_idx >= 1000:
                continue
            host = self.ignite[name].nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    'cd %s; rm -rf %s' % (
                        self.config['rt']['remote']['test_module_dir'],
                        ('%s.server*/work/db/* %s.server*/work/binary_meta/* %sserver*/work/marshaller/*' % (
                            name, name, name)),
                    ),
                ]
        results = self.ssh.exec(commands)
        log_put("Ignite LFS for ignite %s deleted in %s sec" % (name, int(time()-started)))
        log_print()

    def exists_stored_lfs(self, tag, dir_path=None):
        log_put("Looking up stored Ignite LFS tagged '%s' ... " % tag)
        if dir_path is None:
            dir_path = self.config['remote']['suite_var_dir']
        commands = {}
        started = time()
        for node_idx in self.ignite.nodes.keys():
            if node_idx >= 1000:
                continue
            host = self.ignite.nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    'ls %s | grep "ignite_lfs_%s.%s.tar" ' % (
                        dir_path,
                        tag,
                        host,
                    ),
                ]
        results = self.ssh.exec(commands)
        found = True
        for host in results.keys():
            if not 'ignite_lfs_%s' % tag in ''.join(results[host]):
                found = False
                break
        if found:
            log_put("Ignite LFS tagged '%s' found in %s sec" % (tag, int(time()-started)))
        else:
            log_put("Ignite LFS tagged '%s' not found in %s sec" % (tag, int(time()-started)))
        log_print()
        return found

    def preloading(self, name, preloading_size=None):
        if preloading_size is None:
            preloading_size = self.preloading_size
        self.start_simple_clients(
            name,
            '%s/client_default.xml' % self.config['rt']['remote']['test_module_dir'],
            "%s/preloading.*.*" % (self.config['rt']['test_resource_dir'], )
        )
        self.wait_for_entries_num(
            'cache_',
            preloading_size,
            900
        )
        self.log_cache_entries()

    def preloading_with_lfs(self):
        started = time()
        log_print('Preloading with LFS')
        if self.exists_stored_lfs('tiden_preloading'):
            log_print('Stored LFS found, start restoring')
            self.stop_grid()
            self.delete_lfs()
            self.restore_lfs('tiden_preloading')
            self.start_grid()
            log_print('LFS restored in %s sec' % int(time()-started))
        else:
            log_print('Stored LFS not found, start data loading')
            self.preloading()
            self.cu.deactivate()
            self.save_lfs('tiden_preloading')
            self.cu.activate()
            log_print('Data loaded in %s sec' % int(time()-started))

    def grep_in_latest_node_log(self, node_type, grep_pattern):
        node_index = self.ignite.get_last_node_id(node_type)
        output = self.ignite.grep_in_node_log(node_index, grep_pattern)
        return output

