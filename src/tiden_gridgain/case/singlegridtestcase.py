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
from random import random

from tiden.sshpool import SshPool
from tiden.assertions import tiden_assert
from tiden.dockermanager import DockerManager
from tiden.case.generaltestcase import GeneralTestCase
from tiden.ignite import Ignite
from ..piclient.helper.class_utils import ModelTypes
from ..piclient.helper.operation_utils import create_streamer_operation, create_async_operation, \
    create_checksum_operation, create_sum_operation, \
    create_put_all_operation, create_transactional_put_all_operation, \
    create_distributed_checksum_operation, \
    start_dynamic_caches_operation
from ..piclient.piclient import PiClient, PiClientStarter, get_gateway
from tiden.tidenexception import *
from tiden.util import *
from tiden.utilities.control_utility import ControlUtility
from ..utilities.snapshot_utility import SnapshotUtility
from os import path, mkdir

import json
import sys
from threading import Thread

from tiden.report.steps import step


class SingleGridTestCase(GeneralTestCase):
    """
    The base class for tests with single grid.
    The main purpose is start grid on hosts
    """

    ignite: Ignite = None
    preloading_size = 120000
    # run_info = None

    def __init__(self, config, ssh):
        super().__init__(config, ssh)
        self.client_starter = None
        self.need_delete_lfs_on_teardown = True
        if config.get('rt'):
            self.ignite = Ignite(config, ssh, parent_cls=self)
            self.cu = ControlUtility(self.ignite, parent_cls=self)
            self.su = SnapshotUtility(self.ignite, parent_cls=self)

    def setup(self):
        if self.ignite:
            self.ignite.setup()

        self.rebuild_configs()

        super().setup()

    def rebuild_configs(self):
        for name, test_context in self.contexts.items():
            test_context.set_client_result_config("client_%s.xml" % name)
            test_context.set_server_result_config("server_%s.xml" % name)

            test_context.build_config()

    def set_current_context(self, context_name='default'):
        super().set_current_context(context_name)
        self.ignite.set_node_option('*', 'config', self.get_server_config())

    @step()
    def start_grid(self, **kwargs):
        self.start_grid_no_activate(**kwargs)
        if not kwargs.get('skip_activation'):
            self.cu.activate(**kwargs)

    @step()
    def start_grid_no_activate(self, **kwargs):
        self.ignite.start_nodes(**kwargs)

    @step()
    def stop_grid(self, fail=True):
        try:
            self.cu.deactivate()
            sleep(5)
        except AssertionError as e:
            print_red('Catch assertion during deactivate: %s' % str(e))
            if fail:
                raise e
        print('Going to stop nodes....')
        try:
            self.ignite.stop_nodes()
            sleep(5)
        except TidenException as e:
            print_red('Catch exception during stop_nodes: %s' % str(e))
            if fail:
                raise e
            self.ignite.kill_nodes()

    @step()
    def stop_grid_hard(self):
        print('Going to stop nodes....')
        self.ignite.stop_nodes(force=True)
        sleep(5)

    @step()
    def restart_grid(self, with_activation=True):
        self.cu.deactivate()
        sleep(5)
        self.ignite.stop_nodes()
        sleep(5)
        self.ignite.start_nodes()
        if with_activation:
            self.cu.activate(timeout=60)

    @step()
    def restart_empty_grid(self, **kwargs):
        self.cu.deactivate()
        sleep(5)
        self.ignite.stop_nodes()
        sleep(5)
        self.delete_lfs(**kwargs)
        self.ignite.start_nodes()
        self.cu.activate()

    def log_cache_entries(self):
        cache_names = self.ignite.get_cache_names('cache_')
        entry_num = self.ignite.get_entries_num(cache_names)
        log_print("Found %s entries in %s cache(s)" % (entry_num, len(cache_names)), 2)

    def wait_for_entries_num(self, cache_name_prefix, size, timeout, cond='=='):
        """
        The method is waiting for total number of entries for all selected caches
        :param cache_name_prefix:   prefix of cache name
        :param size:                awaited size
        :param timeout:             timeout (in sec)
        :return:
        """
        cache_names = self.ignite.get_cache_names(cache_name_prefix)
        log_put("Waiting: %s caches found" % len(cache_names))
        # Main loop
        started = int(time())
        timeout_counter = 0
        current_size = 0
        matched = False
        while timeout_counter < timeout:
            current_size = self.ignite.get_entries_num(cache_names)
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

    def start_additional_nodes(self, range_to, **kwargs):
        self.ignite.start_additional_nodes(range_to, **kwargs)

    def start_common_nodes(self, ignite_config, num=1, **kwargs):
        if self.client_starter is None:
            self.client_starter = PiClientStarter(self.ignite)
        return self.client_starter.start_common_nodes(ignite_config, num, **kwargs)

    @step()
    def start_simple_clients(self, *args, **kwargs):
        # TODO: Refactor this method
        print('Start simple client %s' % ' '.join(args))
        ignite_config = args[0]
        glob_path = args[1]
        dump_suffix = None
        if len(args) == 3:
            dump_suffix = args[2]
        files = glob(glob_path)
        for file in files:
            file_name = path.basename(file)
            client_args = [
                '-threads=8',
                "-config=%s" % ignite_config,
                "-operation-file=%s/%s" % (self.config['rt']['remote']['test_module_dir'], file_name),
                "-operation=sleep:10"
            ]
            message = "processing %s" % file_name
            if dump_suffix is not None or kwargs.get('dump_file') is not None:
                dump_file = '%s.%s' % (file_name, dump_suffix)
                if kwargs.get('dump_file') is not None:
                    dump_file = kwargs.get('dump_file')
                client_args.append(
                    "-dump-file=%s" % dump_file
                )
                message = "processing %s to %s" % (file_name, dump_file)
            self.ignite.start_simple_client(
                client_args,
                message,
                **kwargs
            )
        if kwargs.get('remote_proc_file'):
            client_args = [
                '-threads=8',
                "-config=%s" % ignite_config,
                "-operation-file=%s" % glob_path,
                "-operation=sleep:10"
            ]
            message = "processing %s" % glob_path
            self.ignite.start_simple_client(
                client_args,
                message,
                **kwargs
            )
        if kwargs.get('waiting_timeout') is not None:
            self.wait_for_running_clients_num(0, kwargs.get('waiting_timeout'))

    @step()
    def wait_for_running_clients_num(self, client_num, timeout, ignite=None):
        """
        Waiting for given number of running client nodes
        :param client_num:  number of client nodes
        :param timeout:     timeout in seconds
        :return:
        """
        started = int(time())
        if ignite is None:
            ignite = self.ignite
        timeout_counter = 0
        current_num = 0
        while timeout_counter < timeout:
            current_num = ignite.get_nodes_num('client')
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

    @step(attach_parameters=True)
    def save_lfs(self, tag, dir_path=None, timeout=SshPool.default_timeout):
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
        for node_idx in self.ignite.nodes.keys():
            if node_idx >= 1000:
                continue
            host = self.ignite.nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    # 'cd %s; zip --symlinks -r ignite_lfs_%s.%s.zip %s' % (
                    'cd %s; tar -cf ignite_lfs_%s.%s.tar %s' % (
                        self.config['rt']['remote']['test_module_dir'],
                        tag,
                        host,
                        '*server*/work/db/* *server*/work/binary_meta/* *server*/work/marshaller/*',
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
        results = self.ssh.exec(commands, timeout=timeout)
        total_size = 0
        for host in results.keys():
            lines = results[host][2]
            m = search('^([0-9]+)\n', lines)
            if m:
                total_size += int(m.group(1))
        log_put("Ignite LFS stored in '%s' in %s sec, size: %s bytes" % (
            tag, int(time() - started), "{:,}".format(total_size))
                )
        log_print()

    @step(attach_parameters=True)
    def restore_lfs(self, tag, dir_path=None, timeout=SshPool.default_timeout):
        log_put("Restore Ignite LFS from '%s' ... " % tag)
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
        results = self.ssh.exec(commands, timeout=timeout)
        log_put("Ignite LFS restored from '%s' in %s sec" % (tag, int(time() - started)))
        log_print()

    @step()
    def delete_lfs(self, delete_db=True, delete_binary_meta=True, delete_marshaller=True, delete_snapshots=True,
                   delete_wal=True, node_ids=None):
        log_put("Delete Ignite LFS ... ")
        commands = {}
        started = time()
        delete_mask = []
        # TODO: Rework this
        if delete_db:
            delete_mask.append('work/db/*')
        if delete_binary_meta:
            delete_mask.append('work/binary_meta/*')
        if delete_marshaller:
            delete_mask.append('work/marshaller/*')
        if delete_snapshots:
            delete_mask.append('work/snapshot/*')
        if delete_wal:
            delete_mask.append('work/db/wal/node*/*')
            delete_mask.append('work/db/wal/archive/node*/*')
            delete_mask.append('work/db/node*/cp/*')

        if len(delete_mask) > 0:
            nodes_to_cleanup = self.ignite.get_all_default_nodes() if node_ids is None else node_ids

            for node_idx in nodes_to_cleanup:
                host = self.ignite.nodes[node_idx]['host']
                if commands.get(host) is None:
                    commands[host] = [
                        'cd %s; rm -rf %s' % (
                            self.ignite.nodes[node_idx]['ignite_home'],
                            ' '.join(delete_mask),
                        ),
                    ]
                else:
                    commands.get(host).append(
                        'cd %s; rm -rf %s' % (
                            self.ignite.nodes[node_idx]['ignite_home'],
                            ' '.join(delete_mask),
                        ),
                    )
            results = self.ssh.exec(commands)

        log_put("Ignite LFS deleted in %s sec" % int(time() - started))
        log_print()

    @step()
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
            log_put("Ignite LFS tagged '%s' found in %s sec" % (tag, int(time() - started)))
        else:
            log_put("Ignite LFS tagged '%s' not found in %s sec" % (tag, int(time() - started)))
        log_print()
        return found

    def preloading(self, preloading_size=None):
        if preloading_size is None:
            preloading_size = self.preloading_size
        self.start_simple_clients(
            '%s/client_default.xml' % self.config['rt']['remote']['test_module_dir'],
            "%s/preloading.*.*" % (self.config['rt']['test_resource_dir'],)
        )
        self.wait_for_entries_num(
            'cache_',
            preloading_size,
            900
        )
        self.log_cache_entries()

    @step()
    def preloading_with_lfs(self):
        started = time()
        log_print('Preloading with LFS')
        if self.exists_stored_lfs('tiden_preloading'):
            log_print('Stored LFS found, start restoring')
            self.stop_grid()
            self.delete_lfs()
            self.restore_lfs('tiden_preloading')
            self.start_grid()
            log_print('LFS restored in %s sec' % int(time() - started))
        else:
            log_print('Stored LFS not found, start data loading')
            self.preloading()
            self.cu.deactivate()
            self.save_lfs('tiden_preloading')
            self.cu.activate()
            log_print('Data loaded in %s sec' % int(time() - started))

    @step()
    def load_data_with_streamer(self, start_key=0, end_key=1001,
                                buff_size=None,
                                value_type=ModelTypes.VALUE_ACCOUNT.value, jvm_options=None,
                                allow_overwrite=None, config_file=None, ignite=None, nodes_num=None,
                                new_instance=False):
        """
        Preloading based on piclient

        :param start_key: start key
        :param end_key: end key
        :param buff_size: put all buffer size
        :param value_type: e.g 'org.apache.ignite.piclient.model.values.Account'
        :param jvm_options: optional JVM options
        :param allow_overwrite:
        :param config_file:
        :return:
        """
        log_print("Load data into current caches using streamer")
        current_client_num = self.ignite.get_nodes_num('client')

        if ignite is None:
            ignite = self.ignite

        if not config_file:
            config_file = self.get_client_config()

        with PiClient(ignite, config_file, jvm_options=jvm_options, nodes_num=nodes_num, new_instance=new_instance) \
                as piclient:
            cache_names = piclient.get_ignite().cacheNames()
            log_print("Loading %s values per cache into %s caches" % (end_key - start_key, cache_names.size()))

            async_operations = []
            for cache_name in cache_names.toArray():
                async_operation = create_async_operation(create_streamer_operation, cache_name, start_key, end_key,
                                                         buff_size=buff_size,
                                                         value_type=value_type,
                                                         parallel_operations=1, allow_overwrite=allow_overwrite)
                async_operations.append(async_operation)
                async_operation.evaluate()

            for async_op in async_operations:
                async_op.getResult()

        log_print("Preloading done")
        self.wait_for_running_clients_num(current_client_num, 90, ignite=ignite)

    @step()
    def load_random_data_with_streamer(self, start_key=0, end_key=1000,
                                       value_type=ModelTypes.VALUE_ACCOUNT.value, jvm_options=None,
                                       allow_overwrite=True, nodes_num=None, ignite=None):
        """
        Preloading based on piclient

        :param start_key: start key
        :param end_key: end key
        :param value_type: e.g 'org.apache.ignite.piclient.model.values.Account'
        :param jvm_options: optional JVM options
        :param allow_overwrite: allowOverwrite flag
        :return:
        """
        from random import randint
        if ignite is None:
            ignite = self.ignite

        log_print("Load random data to caches from start to rand(start, end)")

        current_client_num = ignite.get_nodes_num('client')

        with PiClient(ignite, self.get_client_config(), jvm_options=jvm_options, nodes_num=nodes_num) as piclient:
            cache_names = piclient.get_ignite().cacheNames()
            log_print("Loading %s values per cache into %s caches" % (end_key - start_key, cache_names.size()))

            async_operations = []
            for cache_name in cache_names.toArray():
                rand_start = randint(start_key, end_key)
                async_operation = create_async_operation(create_streamer_operation, cache_name, start_key,
                                                         rand_start,
                                                         value_type=value_type,
                                                         parallel_operations=1, allow_overwrite=allow_overwrite)
                async_operations.append(async_operation)
                async_operation.evaluate()

            for async_op in async_operations:
                async_op.getResult()

        self.wait_for_running_clients_num(current_client_num, 90, ignite=ignite)
        log_print("Random loading done")

    @step()
    def load_data_with_putall(self, start_key=0, end_key=1001,
                              value_type=ModelTypes.VALUE_ACCOUNT.value, jvm_options=None,
                              config_file=None):
        """
        Preloading based on piclient

        :param start_key: start key
        :param end_key: end key
        :param value_type: e.g 'org.apache.ignite.piclient.model.values.Account'
        :param jvm_options: optional JVM options
        :param config_file:
        :return:
        """
        log_print("Load current caches using putAll operation")
        current_client_num = self.ignite.get_nodes_num('client')

        if not config_file:
            config_file = self.get_client_config()

        with PiClient(self.ignite, config_file, jvm_options=jvm_options) as piclient:
            cache_names = piclient.get_ignite().cacheNames()
            log_print("Loading %s values per cache into %s caches" % (end_key - start_key, cache_names.size()))

            async_operations = []
            for cache_name in cache_names.toArray():
                async_operation = create_async_operation(create_put_all_operation, cache_name, start_key, end_key, 100,
                                                         value_type=value_type)
                async_operations.append(async_operation)
                async_operation.evaluate()

            for async_op in async_operations:
                async_op.getResult()

        log_print("Preloading done")
        self.wait_for_running_clients_num(current_client_num, 90)

    @step()
    def start_caches_dynamic(self, jvm_options=None, config_file=None,
                             caches_file_name='caches.xml', batch_size=500):
        """
        Dynamic caches start

        :param caches_file_name: name of cache list file
        :param jvm_options: optional JVM options
        :param config_file: config file to node, which execute dynamic cache start
        :param batch_size: size of cache start block
        :return:
        """
        if not config_file:
            config_file = self.get_client_config()

        file_path = self.ignite.config['rt']['remote']['test_module_dir'] + '/' + caches_file_name

        log_print('Start caches from file - %s' % file_path, color='green')

        count = 0
        with PiClient(self.ignite, config_file, jvm_options=jvm_options, nodes_num=1):
             count = start_dynamic_caches_operation(filepath=file_path, batch_size=batch_size).evaluate()

        if count > 0:
            log_print('Success start %s caches' % count, color='green')
        else:
            raise TidenException("No cache start from file - %s" % file_path)

    @step()
    def calc_checksums_distributed(self, config_file=None, jvm_options=None, validate_empty_data=True):
        """
        Calculate checksum based on piclient

        :param config_file: client config
        :param jvm_options: jvm options
        :param validate_empty_data: validate that there is data on cluster (default True)
        :return:
        """
        log_print("Calculating checksums using distributed job")
        current_client_num = self.ignite.get_nodes_num('client')

        if not config_file:
            config_file = self.get_client_config()

        with PiClient(self.ignite, config_file, jvm_options=jvm_options):
            checksums = create_distributed_checksum_operation().evaluate()

        log_print('Calculating checksums done')

        self.wait_for_running_clients_num(current_client_num, 90)

        # Validate that data exists on cluster
        if validate_empty_data:
            m = re.search('Cluster rows: (\d+) keys', checksums)
            if m:
                cluster_rows = int(m.group(1))

                assert cluster_rows != 0, "There is no data in cluster after load"
            else:
                log_print("Unable to parse checksum utility output!", color='red')

        return checksums

    @step('Calc checksums on client')
    def calc_checksums_on_client(self, start_key=0,
                                 end_key=1000,
                                 dict_mode=False,
                                 config_file=None, jvm_options=None, allow_exception=True):
        """
        Calculate checksum based on piclient

        :param start_key: start key
        :param end_key: end key
        :param dict_mode:
        :param config_file: client config
        :param jvm_options: jvm options
        :return:
        """
        log_print("Calculating checksums using cache.get() from client")
        cache_operation = {}
        cache_checksum = {}

        current_client_num = self.ignite.get_nodes_num('client')

        if not config_file:
            config_file = self.get_client_config()

        with PiClient(self.ignite, config_file, jvm_options=jvm_options) as piclient:
            sorted_cache_names = []
            for cache_name in piclient.get_ignite().cacheNames().toArray():
                sorted_cache_names.append(cache_name)

            sorted_cache_names.sort()

            async_operations = []
            for cache_name in sorted_cache_names:
                try:
                    async_operation = create_async_operation(create_checksum_operation, cache_name, start_key, end_key)
                    async_operations.append(async_operation)
                    cache_operation[async_operation] = cache_name
                    async_operation.evaluate()
                except Exception as e:
                    if allow_exception:
                        raise e

            checksums = ''

            for async_operation in async_operations:
                try:
                    result = str(async_operation.getResult())
                except Exception as e:
                    result = ''
                    if allow_exception:
                        raise e
                cache_checksum[cache_operation.get(async_operation)] = result
                checksums += result

        log_print('Calculating checksums done')

        self.wait_for_running_clients_num(current_client_num, 90)

        if dict_mode:
            return cache_checksum
        else:
            return checksums

    @step()
    def calculate_sum_by_field(self, start_key=0,
                               end_key=1001,
                               field='balance',
                               config_file=None,
                               debug=False,
                               jvm_options=None):
        """
        Calculate checksum based on piclient

        :param start_key: start key
        :param end_key: end key
        :param field: class field name
        :param config_file: client config
        :param debug: print output
        :param jvm_options: jvm options
        :return:
        """
        log_print("Calculating sum over field '%s' in each cache value" % field)

        current_client_num = self.ignite.get_nodes_num('client')

        result = {}

        if not config_file:
            config_file = self.get_client_config()

        with PiClient(self.ignite, config_file, jvm_options=jvm_options) as piclient:
            sorted_cache_names = []
            for cache_name in piclient.get_ignite().cacheNames().toArray():
                sorted_cache_names.append(cache_name)

            sorted_cache_names.sort()

            if debug:
                print(sorted_cache_names)

            total = 0
            for cache_name in sorted_cache_names:
                operation_result = create_sum_operation(cache_name, start_key, end_key, field).evaluate()

                total += operation_result
                result[cache_name] = operation_result

                if debug:
                    print(cache_name)
                    print_blue('Bal %s' % operation_result)

            log_print('Calculating over field done. Total sum: %s' % total)

        self.wait_for_running_clients_num(current_client_num, 90)

        return result

    def grep_in_latest_node_log(self, node_type, grep_pattern):
        node_index = self.ignite.get_last_node_id(node_type)
        output = self.ignite.grep_in_node_log(node_index, grep_pattern)
        return output

    def copy_file_on_host(self, host, from_file, to_file):
        command = 'cp %s %s' % (from_file, to_file)
        output = self.ssh.exec_on_host(host, [command])
        return output

    @step()
    def verify_no_assertion_errors(self):
        assertion_errors = self.ignite.find_exception_in_logs(".*java.lang.AssertionError.*")

        # remove assertions from ignite.nodes to prevent massive output
        for node_id in self.ignite.nodes.keys():
            if 'exception' in self.ignite.nodes[node_id] and self.ignite.nodes[node_id]['exception'] != '':
                log_print("AssertionError found on node %s, text: %s" %
                          (node_id, self.ignite.nodes[node_id]['exception'][:100]),
                          color='red')
                self.ignite.nodes[node_id]['exception'] = ''

        assert assertion_errors == 0, "AssertionErrors found in server logs! Count %d" % assertion_errors

    def util_copy_piclient_model_to_libs(self):
        cmd = [
            'cp %s %s/libs/' % (self.ignite.config['artifacts']['piclient']['remote_path'],
                                self.ignite.config['artifacts']['ignite']['remote_path'])
        ]
        self.util_exec_on_all_hosts(cmd)

    def util_copy_test_tools_to_libs(self):
        cmd = [
            'cp %s %s/libs/' % (self.ignite.config['artifacts']['test_tools']['remote_path'],
                                self.ignite.config['artifacts']['ignite']['remote_path'])
        ]
        self.util_exec_on_all_hosts(cmd)

    def util_copy_piclient_and_test_tools_models_to_libs(self):
        cmd = \
            ['cp %s %s/libs/' % (self.ignite.config['artifacts']['piclient']['remote_path'],
                                 self.ignite.config['artifacts']['ignite']['remote_path'])
             ]
        if self.ignite.config['artifacts'].get('test_tools'):
            cmd.extend(
                ['cp %s %s/libs/' % (self.ignite.config['artifacts']['test_tools']['remote_path'],
                                     self.ignite.config['artifacts']['ignite']['remote_path'])
                 ])
        self.util_exec_on_all_hosts(cmd)

    def create_loading_metrics_graph(self, file_name, metrics, **kwargs):
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        attr_color = {
            "start": 'black',
            "end": 'black',
            " ": 'green'
        }
        plot_colors = ['b', 'g', 'r', 'c', 'm']

        fig, ax = plt.subplots()

        plt.style.use('ggplot')

        counter = 0
        for metric_name in metrics:
            metric_value = metrics[metric_name]

            if metric_name != 'custom_events':
                ax.plot(metric_value[0], metric_value[1], plot_colors[counter % 6], label=metric_name)
                counter += 1

        ax.legend(loc='upper left', shadow=True, fontsize=8)

        if 'custom_events' in metrics:
            custom_events_lines = []

            custom_events = metrics['custom_events']
            for id_event in range(0, len(custom_events[0])):
                custom_events_lines.append('%s. %s (%s)' % (str(int(id_event + 1)),
                                                            custom_events[2][id_event],
                                                            custom_events[3][id_event]))
                plt.annotate('%s' % str(int(id_event + 1)),
                             xy=(custom_events[0][id_event], custom_events[1][id_event]),
                             arrowprops=dict(facecolor='black', shrink=0.05), )

            plt.text(0, 50, '\n'.join(custom_events_lines), fontsize=8)

        plt.xlabel('time, s')
        plt.ylabel('transactions')
        plt.title('Transactions performance')
        plt.grid(True)
        plt.tight_layout(pad=0.1, w_pad=0.1, rect=(0, 0, 1, 1))

        if not path.exists('%s/test-plots' % self.config['suite_var_dir']):
            mkdir('%s/test-plots' % self.config['suite_var_dir'])

        fig = plt.gcf()
        dpi_factor = kwargs.get('dpi_factor', 1)
        fig.set_size_inches(22.5, 10.5)
        plt.savefig('%s/test-plots/plot-%s.png' % (self.config['suite_var_dir'], file_name),
                    dpi=1000 * dpi_factor,
                    orientation='landscape')

    def util_exec_on_all_hosts(self, commands_to_exec, print_results=False):
        commands = {}
        hosts = self.ignite.config['environment'].get('server_hosts', []) + \
                self.ignite.config['environment'].get('client_hosts', [])

        for host in hosts:
            if commands.get(host) is None:
                commands[host] = commands_to_exec

        results = self.ssh.exec(commands)

        if print_results:
            log_print(results, color='red')

    def run_console_thread(self, ignite, frequency=5):
        self._console_thread(self.ignite, frequency).start()

    class _console_thread(Thread):
        ignite = None
        frequency = 5
        support_command = {'thread': 'thread',
                           'jfr': 'jfr [duration]',
                           'help': 'help'}

        def __init__(self, ignite, frequency):
            Thread.__init__(self)
            self.setDaemon(True)
            self.ignite = ignite
            self.frequency = frequency
            log_print("Run console interactive parallel thread - available command: %s" % self.support_command.values(),
                      color='red')

        def run(self):
            while True:
                try:
                    for line in sys.stdin:
                        if line.rstrip().lower().rsplit(' ')[0] in self.support_command.keys():
                            if line.rstrip().lower().startswith('thread'):
                                self.ignite.make_cluster_thread()
                            if line.rstrip().lower().startswith('help'):
                                log_print('help:\navailable command: %s' % self.support_command.values(), color='red')
                            if line.rstrip().lower().startswith('jfr'):
                                duration = 30
                                try:
                                    duration = int(line.rstrip().lower().rsplit(' ')[1])
                                except Exception:
                                    pass
                                self.ignite.make_cluster_jfr(duration)
                        else:
                            log_print('Unsupported command - %s' % line, color='red')
                    sleep(self.frequency)
                except Exception as e:
                    log_print("Got Exception in console read worker: %s" % str(e), color='red')

    def _get_tx_type_map(self, gateway):
        concurrency_isolation_map = {
            'OPTIMISTIC': gateway.jvm.org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC,
            'PESSIMISTIC': gateway.jvm.org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC,
            'READ_COMMITTED': gateway.jvm.org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED,
            'REPEATABLE_READ': gateway.jvm.org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ,
            'SERIALIZABLE': gateway.jvm.org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE,
        }
        return concurrency_isolation_map

    def util_deploy_sbt_model(self):
        """
        Deploy sbt_model.zip to all servers. sbt_model.zip contains:
            model.jar - should be placed to ignite/libs folder;
            caches.xml - should be renamed to caches_sbt.xml and placed to remote resource dir;
            json_model.json - should be placed to remote resource dir.
        """
        if self.get_context_variable('sbt_model_enabled'):
            sbt_model_remote = self.ignite.config['artifacts']['sbt_model']['remote_path']
            remote_res = self.ignite.config['rt']['remote']['test_module_dir']
            cmd = [
                'cp %s/model.jar %s/libs/' % (sbt_model_remote, self.ignite.config['artifacts']['ignite']['remote_path']),
                'cp %s/caches.xml %s/caches_sbt.xml' % (sbt_model_remote, remote_res),
                'cp %s/json_model.json %s' % (sbt_model_remote, remote_res),

            ]
            self.util_exec_on_all_hosts(cmd)

            server_host = self.ignite.config['environment'].get('server_hosts', [])[0]
            self.ssh.download_from_host(server_host, '%s/json_model.json' % remote_res,
                                        '%s/json_model.json' % self.ignite.config['rt']['test_resource_dir'])

    # @staticmethod
    # def set_run_info(run_info):
    #     SingleGridTestCase.run_info = run_info

    def get_run_info(self):
        return self.ignite.get_run_info()

    def idle_verify_check_conflicts_action(self, *args):
        try:
            self.ignite.cu.control_utility('--cache', 'idle_verify', '--skip-zeros', '--cache-filter', 'PERSISTENT')
            tiden_assert(
                'idle_verify check has finished, no conflicts have been found.'
                in self.ignite.cu.latest_utility_output,
                "Check no conflicts in partition counters and hashes"
            )
            self.verify_no_assertion_errors()
        except Exception as e:
            self.need_delete_lfs_on_teardown = False
            raise e

    def idle_verify_dump_action(self):
        self.ignite.cu.control_utility('--cache', 'idle_verify', '--dump', '--cache-filter', 'PERSISTENT')

    @step()
    def cleanup_lfs(self, node_id=None):
        log_print('Cleanup %s LFS ' % ("cluster" if not node_id else "node %s" % node_id), color='blue')
        commands = {}

        nodes_to_clean = [node_id] if node_id else self.ignite.nodes.keys()

        for node_idx in nodes_to_clean:
            host = self.ignite.nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    'rm -rf %s/work/*' % self.ignite.nodes[node_idx]['ignite_home']
                ]
            else:
                commands[host].append('rm -rf %s/work/*' % self.ignite.nodes[node_idx]['ignite_home'])
        results = self.ssh.exec(commands)
        log_print(results)
        log_print("Ignite LFS deleted.")

    @step()
    def reset_cluster(self):
        default_server_nodes = self.ignite.get_all_default_nodes()
        tmp_nodes = {}
        for node_idx in self.ignite.nodes.keys():
            if node_idx in default_server_nodes:
                tmp_nodes[node_idx] = dict(self.ignite.nodes[node_idx])
                if tmp_nodes[node_idx].get('log'):
                    del tmp_nodes[node_idx]['log']
                tmp_nodes[node_idx]['run_counter'] = 0
            else:
                log_print('Deleted node {} from nodes'.format(node_idx), color='blue')
        self.ignite.nodes = tmp_nodes

