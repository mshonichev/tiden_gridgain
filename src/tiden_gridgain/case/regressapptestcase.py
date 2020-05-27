#!/usr/bin/env python3

from tiden.case.apptestcase import AppTestCase
from tiden.utilities.control_utility import ControlUtility
from tiden.util import log_print, log_put, render_template
from requests import get as requests_get
from ..utilities.snapshot_utility import SnapshotUtility


class RegressAppTestCase(AppTestCase):

    def __init__(self, *args):
        super().__init__(*args)
        self.add_app('ignite')
        self.add_app('zookeeper')
        self.caches = {}
        self.shared_storage = None

    def setup(self):
        conf = self.tiden.config['environment']
        res_dir = self.tiden.config['rt']['test_resource_dir']
        # Collect Ignite addresses
        addresses = []
        # TODO: wtf?
        for host_type in ['server', 'client']:
            if conf.get("%s_hosts" % host_type):
                instances_per_host = conf.get("%ss_per_host" % host_type, 1)
                for addr in conf["%s_hosts" % host_type]:
                    for isinstance_num in range(0, instances_per_host):
                        if addr not in addresses:
                            addresses.append(addr)
        # Render config files
        zk_enabled = False
        for test_type in ['ignite', 'zk']:
            render_template(
                "%s/*.tmpl.xml" % res_dir,
                test_type,
                {
                    'addresses': addresses,
                    'zookeeper_enabled': zk_enabled
                }
            )
            zk_enabled = True
        super().setup()
        # We need only one ZK node
        zookeeper_app = self.get_app_by_type('zookeeper')[0]
        node1 = zookeeper_app.nodes[1]
        zookeeper_app.nodes = {
            1: node1
        }

    def teardown(self):
        super().teardown()

    # Helper methods

    def run_ignite_cluster(self, restart=False, preloading=True):
        """
        Run Ignite cluster
        :param restart:     Restart cluster during test
        :return:
        """
        cfg = self.tiden.config
        ignite_app = self.get_app_by_type('ignite')[0]
        ignite_app.reset()
        log_print("Ignite ver. %s, revision %s" % (
                cfg['artifacts'][ignite_app.name]['ignite_version'],
                cfg['artifacts'][ignite_app.name]['ignite_revision'],
            )
        )
        ignite_app.start_nodes()
        ignite_app.cu.activate()
        loaded_entries = 0
        if preloading:
            self.caches = ignite_app.get_cache_names()
            loaded_entries = self.preloading_with_rest(ignite_app, 200)
        if restart:
            log_print("Ignite cluster restart")
            ignite_app.cu.deactivate()
            ignite_app.stop_nodes()
            ignite_app.start_nodes()
            ignite_app.cu.activate()
        if preloading:
            found_entries = ignite_app.get_entries_num(self.caches)
            assert loaded_entries == found_entries, "Entries number after cluster restart: expected %s, found %s" % (
                loaded_entries, found_entries
            )
            log_print("%s entries found" % found_entries)

    def stop_ignite_cluster(self):
        ignite_app = self.get_app_by_type('ignite')[0]
        ignite_app.cu.deactivate()
        ignite_app.stop_nodes()
        ignite_app.delete_lfs()

    def preloading_with_rest(self, ignite, num_limit_per_cache):
        """
        Load data into all caches by REST PUTALL command
        https://apacheignite.readme.io/docs/rest-api#section-put-all
        :param ignite:              Ignite instance
        :param num_limit_per_cache  Number of entries per cache base
        :return:                    Number of loaded entries
        """
        url = "http://%s:%s/ignite?cmd=putall&cacheName={cache}&{entries}" % (
            ignite.nodes[1]['host'], ignite.nodes[1]['rest_port'])
        put_data = {1: ""}
        put_id = 0
        key_id = 1
        for id in range(1, num_limit_per_cache+1):
            # Split entries into groups due to limit due to GET url length
            if ((id-1) % 100) == 0:
                put_id += 1
                put_data[put_id] = ""
                key_id = 1
            put_data[put_id] += "k{key_id}={id}&v{key_id}={id}&".format(id=id, key_id=key_id)
            key_id += 1
        entry_num = 0
        cache_idx = 0
        throttle = 0
        for cache in self.caches:
            entry_num += num_limit_per_cache
            for put_id in sorted(put_data.keys()):
                full_url = url
                full_url = full_url.format(cache=cache, entries=put_data[put_id][:-1])
                response = requests_get(full_url)
                assert response.status_code == 200, \
                    "Returned code for request %s: expected 200, found %s" % (full_url, response.status_code)
                json_reply = response.json()
                assert json_reply['successStatus'] == 0, \
                    "JSON reply 'successStatus' for request %s: expected 0, found %s" % (
                        full_url, json_reply['successStatus'])
            cache_idx += 1
            if not (throttle % 13):
                log_put("Loading data: %s/%s caches processed, %s entries loaded" % (
                    cache_idx, len(self.caches), entry_num)
                        )
            throttle = throttle + 1
        log_print()
        return entry_num

    def get_shared_folder_path(self):
        return self.shared_storage

    def create_shared_snapshot_storage(self):
        ignite_app = self.get_app_by_type('ignite')[0]
        self.shared_storage = ignite_app.su.create_shared_snapshot_storage(unique_folder=True, prefix='regress')
        return self.get_shared_folder_path()

    def remove_shared_snapshot_storage(self):
        ignite_app = self.get_app_by_type('ignite')[0]
        return ignite_app.su.remove_shared_snapshot_storage(self.get_shared_folder_path())

    def setup_shared_folder_test(self):
        self.create_shared_snapshot_storage()

    def teardown_shared_folder_test(self):
        self.tiden.ssh.killall('java')
        self.remove_shared_snapshot_storage()
