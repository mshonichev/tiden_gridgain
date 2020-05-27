#!/usr/bin/env python3

from time import sleep
from collections import namedtuple

from tiden.apps.zookeeper import Zookeeper
from .singlegridtestcase import SingleGridTestCase
from tiden.util import version_num, get_from_version_dict, is_enabled


class SingleGridZooTestCase(SingleGridTestCase):
    """
    The base class for tests with single grid and zookeeper.
    The main purpose is start zookeeper and grid on hosts.
    """

    # ignite = None
    zoo: Zookeeper = None

    def __init__(self, config, ssh):
        super().__init__(config, ssh)
        self.zoo = Zookeeper(config, ssh)
        self.ssl_enabled = None
        self.ssl_conn_tuple = None

    def setup(self):

        if not self.ssl_enabled:
            self.ssl_enabled = is_enabled(self.config.get('ssl_enabled'))

        if self.ssl_enabled:
            keystore_pass = truststore_pass = '123456'
            ssl_config_path = self.config['rt']['remote']['test_module_dir']
            ssl_params = namedtuple('ssl_conn', 'keystore_path keystore_pass truststore_path truststore_pass')

            self.ssl_conn_tuple = ssl_params(keystore_path='{}/{}'.format(ssl_config_path, 'server.jks'),
                                             keystore_pass=keystore_pass,
                                             truststore_path='{}/{}'.format(ssl_config_path, 'trust.jks'),
                                             truststore_pass=truststore_pass)

            self.cu.enable_ssl_connection(self.ssl_conn_tuple)
            self.su.enable_ssl_connection(self.ssl_conn_tuple)

            for context_name in self.contexts:
                self.contexts[context_name].add_context_variables(
                    ssl_enabled=True,
                    ssl_config_path=ssl_config_path,
                )

        super().setup()
        if self.get_context_variable('zookeeper_enabled'):
            self.zoo.deploy_zookeeper()

    def start_zookeeper(self):
        self.zoo.start()

    def stop_zookeeper(self):
        self.zoo.stop()

    def stop_grid_hard(self):
        print('Going to stop nodes....')
        self.ignite.stop_nodes(force=True)
        sleep(5)

    def run_snapshot_utility(self, *args, **kwargs):
        """
        This is a wrapper for snapshot utility function. All high level logic should be done here.
        :param args:
        :param kwargs:
        :return:
        """
        args = list(args)
        context_variables = self.contexts['default'].get_context_variables()

        # if command is 'snapshot' and arch_enabled is True add -archive option.
        if context_variables.get('snapshot_archive') and str(args[0]).lower() in ['snapshot']:
            if not kwargs.get('do_not_archive'):
                args.append('-archive=ZIP')

        if context_variables.get('use_copy') and str(args[0]).lower() in ['move']:
            args[0] = 'copy'

        if context_variables.get('single_copy') and str(args[0]).lower() in ['move', 'copy']:
            args.append('-single_copy')

        if len(args) > 1 and '-force' in str(args[1]).lower():
            args = self.handle_versioned_force(args)

        self.su.snapshot_utility(*args, **kwargs)

    def handle_versioned_force(self, args):
        force_behaviour = {
            '0': {
                'args': '-force'
            },
            '8.5.1-p150': {
                'args': '-chain=FROM'
            }
        }

        gg_version = self.config['artifacts']['ignite']['gridgain_version']
        expected_arg = get_from_version_dict(force_behaviour, gg_version).get('args')

        return [arg.replace('-force', expected_arg) for arg in args]

    @staticmethod
    def get_from_version_dict(version_dict, version):
        """
        Returns value from versioned dict. If exact version is not found in dict, then value for nearest version
        which is less than current will be returned.
        For example:
        version_dict = {'0': 'some_value', '2.5.1-p160': 'another values'}
        for version 2.5.1-p150 will be returned value for '0' key;
        for version 2.5.1-p161 will be returned value for '2.5.1-p160' key.

        :param version_dict:
        :param version:
        :return:
        """
        exact_value = version_dict.get(version)
        if not exact_value:
            exact_value = version_dict.get(SingleGridZooTestCase.util_get_nearest_version(version_dict, version))
        return exact_value

    @staticmethod
    def util_get_nearest_version(version_dict, version):
        ver_mapping = {}
        versions_to_check = []
        for ver in version_dict.keys():
            converted_version = version_num(ver)
            ver_mapping[converted_version] = ver
            versions_to_check.append(converted_version)

        versions_to_check.sort()
        exact_version = [ver for ver in versions_to_check if ver < version_num(version)][-1]
        return ver_mapping[exact_version]

    def get_snapshot_id(self, snapshot_number):
        snapshot_id = None
        if snapshot_number <= len(self.su.snapshots):
            snapshot_id = self.su.snapshots[snapshot_number - 1]['id']
        return snapshot_id

    def get_latest_snapshot_id(self):
        snapshot_id = None
        if len(self.su.snapshots) > 0:
            snapshot_id = self.su.snapshots[-1]['id']
        return snapshot_id
