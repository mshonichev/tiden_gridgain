import re
from random import randint

from ..piclient.helper.cache_utils import IgniteCache
from ..piclient.piclient import PiClient
from ..util import print_blue, print_red, log_put, log_print
from .singlegridzootescase import SingleGridZooTestCase
from time import sleep


class RegressTestCase(SingleGridZooTestCase):
    preloading_size = 60000
    max_key = 1001
    shared_storage = None

    def setup_test(self):
        self.ignite.set_node_option('*', 'config', self.get_server_config())
        self.ignite.set_node_option('*', 'jvm_options', self.get_default_jvm_options())
        self.util_copy_piclient_and_test_tools_models_to_libs()

    def teardown_test(self):
        self.stop_grid(fail=False)
        self.delete_lfs(node_ids=self.ignite.nodes.keys())
        self.reset_cluster()

    def teardown_test_hard(self):
        self.stop_grid_hard()
        self.delete_lfs()

    def setup_pitr_test(self):
        self.setup_test()
        self.ignite.set_node_option('*', 'jvm_options', self.get_pitr_enabled_jvm_options())

    def setup_pmi_pitr_test(self):
        self.setup_test()
        self.ignite.set_node_option('*', 'jvm_options', self.get_pmi_pitr_enabled_jvm_options())

    def teardown_pitr_test(self):
        self.teardown_test()

    def setup_shared_storage_test(self):
        self.setup_test()
        self.create_shared_snapshot_storage()

    def get_shared_storage(self):
        return self.shared_storage

    def teardown_shared_storage_test(self):
        self.teardown_test()
        # this should be done at the end as this call most likely will fail.
        self.remove_shared_snapshot_storage()

    def setup_pitr_shared_storage_test(self):
        self.setup_pitr_test()
        self.create_shared_snapshot_storage()

    def teardown_pitr_shared_storage_test(self):
        self.teardown_pitr_test()
        # this should be done at the end!!!
        self.remove_shared_snapshot_storage()

    def get_default_jvm_options(self):
        return [
            '-DWAL_HISTORY_SIZE=20',
            '-DWAL_TLB_SIZE=0',  # default is uninitialized, which in turn falls back to walSegmentSize/4
            '-DCHECKPOINTING_FREQUENCY=1000',
            '-DWAL_MODE=LOG_ONLY',
            '-DPOINT_IN_TIME_RECOVERY_ENABLED=false',
            '-DROLLING_UPDATES_ENABLED=false',
        ]

    def get_extended_jvm_options(self, options):
        new_options = []
        for option in options:
            if '=' in option:
                option_name, option_value = option.split('=')
                new_options.append(option_name)
        result = []
        for option in self.get_default_jvm_options():
            if '=' in option:
                option_name, option_value = option.split('=')
                if option_name not in new_options:
                    result.append(option)
            else:
                result.append(option)
        result.extend(options)
        return result

    def get_async_io_jvm_options(self):
        return self.get_extended_jvm_options([
            '-DIGNITE_USE_ASYNC_FILE_IO_FACTORY=true',
        ])

    def get_pitr_enabled_jvm_options(self):
        return self.get_extended_jvm_options([
            '-DPOINT_IN_TIME_RECOVERY_ENABLED=true',
            '-DGG_SNAPSHOT_AFTER_CLUSTER_STATE_CHANGE=true',
        ])

    # few tweaks to mimick Sberbank prom settings
    def get_pmi_pitr_enabled_jvm_options(self):
        return self.get_extended_jvm_options([
            '-DWAL_HISTORY_SIZE=2147483647',
            '-DCHECKPOINTING_FREQUENCY=180000',
            '-DROLLING_UPDATES_ENABLED=true',
            '-DWAL_TLB_SIZE=52425880',
            '-DPOINT_IN_TIME_RECOVERY_ENABLED=true',
            '-DGG_SNAPSHOT_AFTER_CLUSTER_STATE_CHANGE=true',
        ])

    def get_snapshot_id(self, snapshot_number):
        snapshot_id = None
        if snapshot_number <= len(self.su.snapshots):
            snapshot_id = self.su.snapshots[snapshot_number - 1]['id']
        return snapshot_id

    def find_exception_in_logs(self, exception, result_node_option='exception', node_id=None):
        self.ignite.find_exception_in_logs(exception, result_node_option=result_node_option)
        exceptions_cnt = 0
        nodes_to_check = self.ignite.nodes.keys()
        if node_id:
            nodes_to_check = [node_id]

        for node_idx in nodes_to_check:
            if result_node_option in self.ignite.nodes[node_idx] \
                    and self.ignite.nodes[node_idx][result_node_option] is not None \
                    and self.ignite.nodes[node_idx][result_node_option] != '':
                print_red(self.ignite.nodes[node_idx][result_node_option])
                exceptions_cnt = exceptions_cnt + 1
        return exceptions_cnt

    def get_last_snapshot_id(self):
        return self.get_snapshot_id(len(self.su.snapshots))

    def get_shared_folder_path(self):
        return self.shared_storage

    def create_shared_snapshot_storage(self):
        self.shared_storage = self.ignite.su.create_shared_snapshot_storage(unique_folder=True, prefix='regress')
        return self.get_shared_folder_path()

    def remove_shared_snapshot_storage(self):
        return self.ignite.su.remove_shared_snapshot_storage(self.get_shared_folder_path())

    def remove_random_from_caches(self, easy=False):
        clients_num = self.ignite.get_nodes_num('client')

        with PiClient(self.ignite, self.get_client_config()):
            caches = self.ignite.get_cache_names('cache_group')

            print_blue('--- Caches ---')
            print_blue(caches)

            for cache_name in caches:
                cache = IgniteCache(cache_name)

                if easy:
                    for index in range(1,
                                       randint(1, int(len(caches) / 2))):
                        cache.remove(index, key_type='long')
                else:
                    for index in range(randint(1, int(self.max_key / 2)),
                                       randint(int(self.max_key / 2) + 1, self.max_key)):
                        cache.remove(index, key_type='long')

        self.wait_for_running_clients_num(clients_num, 120)

    def check_on_all_nodes(self, command):
        output = dict()
        for node_id in self.ignite.nodes.keys():
            commands = {}
            if node_id < 1000:
                host = self.ignite.nodes[node_id]['host']
                ignite_home = self.ignite.nodes[node_id]['ignite_home']

                commands[host] = [
                    'cd %s;%s' % (ignite_home, command.replace(
                        '__CONSISTENT_ID__', self.ignite.get_node_consistent_id(node_id)))]
                print_red(commands)
                tmp_output = self.ssh.exec(commands)
                print_blue(tmp_output)
                output[node_id] = tmp_output[host][0]
        return output

    def check_on_node(self, node_idx, command):
        assert node_idx in self.ignite.nodes.keys(), "Non-existent node id %s!" % node_idx
        output = dict()
        commands = {}
        host = self.ignite.nodes[node_idx]['host']
        ignite_home = self.ignite.nodes[node_idx]['ignite_home']

        commands[host] = [
            'cd %s;%s' % (ignite_home, command.replace('__CONSISTENT_ID__',
                                                       self.ignite.get_node_consistent_id(node_idx)))]
        print_red(commands)
        tmp_output = self.ssh.exec(commands)
        print_blue(tmp_output)
        output[node_idx] = tmp_output[host][0]
        return output

    def corrupt_page_store(self, node_idx=1, page_store_dir='work/db/__CONSISTENT_ID__', max_parts=3, max_caches=2):
        """
        corrupt page store by dumping zeros to page-xxx.bin into max_parts files of max_caches caches found on node
        :param node_idx:
        :param page_store_dir:
        :param max_parts:
        :param max_caches:
        :return:
        """
        output = self.check_on_node(node_idx, 'ls -1 %s/' % page_store_dir)
        lines = output[node_idx].split('\n')
        n_caches = 0
        for line in lines:
            if 'cacheGroup' in line:
                n_caches = n_caches + 1
                if n_caches > max_caches:
                    break
                n_parts = 0
                cache_dir = page_store_dir + '/' + line.strip()
                output = self.check_on_node(node_idx, 'ls -1 %s/' % cache_dir)
                lines2 = output[node_idx].split('\n')
                for line2 in lines2:
                    if 'part-' in line2:
                        n_parts = n_parts + 1
                        if n_parts > max_parts:
                            break
                        part_file_name = line2.strip()
                        part = cache_dir + '/' + part_file_name
                        self.check_on_node(node_idx, 'dd if=/dev/zero of=%s bs=1024 count=20 conv=notrunc' % part)

    def corrupt_snapshot(self, snapshot_id, src='work/snapshot/', is_aggregated_snapshot=False):
        output = self.ignite.run_on_all_nodes('ls %s/' % src)
        snapshot_folder_name = None
        for node_id in self.ignite.nodes.keys():
            if node_id < 1000:
                lines = output[node_id].split('\n')
                for line in lines:
                    if '_' + str(snapshot_id) + '.snapshot' in line:
                        snapshot_folder_name = line
                        break

        assert snapshot_folder_name is not None, "Can't find snapshot '%s' folder on any nodes" % snapshot_id
        self.ignite.run_on_all_nodes(
            "dd if=/dev/random of=%s/%s/__CONSISTENT_ID__/snapshot-meta.bin bs=1 count=42 conv=notrunc" % (
                src, snapshot_folder_name)
        )
        if is_aggregated_snapshot:
            host = self.config['environment']['server_hosts'][0]
            command = "dd if=/dev/random of=%s/%s/snapshot-meta.bin bs=1 count=42 conv=notrunc" % (
                src, snapshot_folder_name)
            self.ssh.exec_on_host(host, [command])

    def load_expected_text(self, name):
        expected_file_name = "%s/res/expects/%s" % (self.get_suite_dir(), name)
        with open(expected_file_name) as f:
            text = f.readlines()
            f.close()
            return text
        # return re.compile(''.join(text), re.DOTALL)

    def load_expected_regexp(self, name):
        expected_file_name = "%s/res/expects/%s" % (self.get_suite_dir(), name)
        with open(expected_file_name) as f:
            text = f.readlines()
            f.close()
            return text

    def match_expected(self, expected, output):
        output_lines = output.rstrip('\n').split('\n')
        for line_no, output_line in enumerate(output_lines):
            l = expected[line_no].rstrip('\n')
            r = re.compile(l)
            t = output_line.rstrip('\n')
            m = r.match(t)
            assert m is not None, "expected line '%s' does not match '%s'" % (l, t)
        return True

    def _restart_empty_grid(self):
        self.cu.deactivate()
        sleep(5)
        self.ignite.stop_nodes()
        sleep(5)
        self._cleanup_lfs()
        self.ignite.start_nodes()
        self.cu.activate()

    def _cleanup_lfs(self):
        log_put("Cleanup Ignite LFS ... ")
        commands = {}

        for node_idx in self.ignite.nodes.keys():
            host = self.ignite.nodes[node_idx]['host']
            if commands.get(host) is None:
                commands[host] = [
                    'rm -rf %s/work/*' % self.ignite.nodes[node_idx]['ignite_home']
                ]
            else:
                commands[host].append('rm -rf %s/work/*' % self.ignite.nodes[node_idx]['ignite_home'])
        results = self.ssh.exec(commands)
        print(results)
        log_put("Ignite LFS deleted.")
        log_print()

