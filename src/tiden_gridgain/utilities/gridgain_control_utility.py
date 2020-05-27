#!/usr/bin/env python3

from tiden.utilities import ControlUtility


class GridGainControlUtility(ControlUtility):

    def __init__(self, ignite, parent_cls=None):
        super().__init__(self, ignite, parent_cls=parent_cls)

    def dr(self):
        return DRControlUtility(self.ignite, self.latest_utility_output, self.latest_command)


class DRControlUtilityException(Exception):
    pass


class DRControlUtility(ControlUtility):

    def __init__(self, ignite, latest_output, latest_command):
        super().__init__(self, ignite)
        self.latest_utility_output = latest_output
        self.latest_command = latest_command
        common_commands = {'dc_id': r'Data Center ID: (\d+)'}
        self.commands = {
            'state': {
                'sender_groups': r'Configured sender group\(s\).+\[(.+)\]',
                'receiver_caches': r'Configured (\d+) receiver cache\(s\)\.',
                **common_commands
            },
            'topology': {

                'topology': r'Topology: (\d+) server\(s\), (\d+) client\(s\)',
                'node_info_multiline': r'nodeId=(.+), Address=\[(.+)\]$',
                'sender_hubs_info_multiline': r'nodeId=(.+), Address=\[(.+)\], Mode=(.+)',
                'receiver_hubs_info_multiline': r'nodeId=(.+), Address=(?!\[)(.+)(?!\]), Mode=(.+)',
                'sender_count': r'Sender hubs: (\d+)',
                'receiver_count': r'Receiver hubs: (\d+)',
                'other_nodes': r'Other nodes: (.+)',
                'data_nodes_count': r'Data nodes: (.+)',
                **common_commands
            },
            'node': {
                'addresses': r'Node addresses: \[(.+)\]',
                'mode': r'Mode=(.+)',
                'streamer_pool_size': r'StreamerThreadPoolSize=(\d+)',
                'thread_pool_size': r'\s+ThreadPoolSize=(\d+)',
                **common_commands
            },
            'full-state-transfer': {
                'transferred_caches': r'Full state transfer command completed successfully for caches \[(.+)\]',
                **common_commands
            },
            'cache': {
                'caches_affected': r'(\d+) matching cache\(s\): \[(.+)\]',
                'sender_metrics_multiline': r'Sender metrics for cache \"(.+)\":',
                'receiver_metrics_multiline': r'Receiver metrics for cache \"(.+)\":',
                'receiver_configuration_multiline': r'Receiver configuration for cache \"(.+)\":',
                'sender_configuration_multiline': r'Sender configuration for cache \"(.+)\":',
                **common_commands
            },
            'pause': {
                **common_commands
            },
            'resume': {
                **common_commands
            }
        }

    def parse(self):
        """
        Assert command execution (finished with 0 code)
        Parse latest output with regexes related to latest command

        Result for 'cache' on master cluster with DR on servers looks like:
        {'dc_id': '1',
         'caches_affected': [
            '120',
            'cache_group_1_001, cache_group_1_002, cache_group_1_003, ...'
         ],
         'receiver_configuration': [
            ['cache_group_1_001'],
            ['cache_group_1_002'],
            ['cache_group_1_003'],
            ...
         ],
         'sender_configuration': [
            ['cache_group_1_001'],
            ['cache_group_1_002'],
            ['cache_group_1_003'],
            ...
         ]}
        """
        result = {}
        if 'finished with code: 0' not in self.latest_utility_output:
            self.ignite.logger.debug(self.latest_utility_output)
            raise DRControlUtilityException(f'Command control.sh {" ".join(self.latest_command)} executed with exception')

        for line in self.latest_utility_output.split('\n'):
            for name, pattern in self.commands[self.latest_command[1]].items():
                found = search(pattern, line)
                multiline = False
                if name.endswith('_multiline'):
                    name = name[:-10]
                    multiline = True
                if found:
                    found_list = list(found.groups())
                    if result.get(name):
                        if multiline:
                            result[name] = result[name] + [found_list]
                        else:
                            raise TidenException(f'Many times found {name} pattern')
                    else:
                        if multiline:
                            result[name] = [found_list]
                        else:
                            if len(found_list) == 1:
                                found_list = found_list[0]
                            result[name] = found_list

        sorted_result = {}
        for k, v in result.items():
            if isinstance(v, list) and isinstance(v[0], list):
                sorted_result[k] = sorted(v)
            else:
                sorted_result[k] = v
        return sorted_result
