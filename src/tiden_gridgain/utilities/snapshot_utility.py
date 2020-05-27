from re import search

from tiden import log_print, TidenException, get_logger, print_red, print_blue, print_green, \
    tiden_assert_equal, util_sleep_for_a_while
from tiden.report.steps import step


class SnapshotUtility:

    def __init__(self, ignite, parent_cls=None):
        self.ignite = ignite
        self.snapshots = []
        self.latest_utility_output = None
        self.ssl_connection_string = None
        self.authentication_enabled = False
        self.ssl_connection_enabled = False
        self.auth_login = None
        self.auth_password = None
        self.output_file_content = None
        self.run_count = 0
        self._parent_cls = parent_cls

    def enable_authentication(self, login, password):
        self.authentication_enabled = True
        self.auth_login = login
        self.auth_password = password

    def disable_authentication(self):
        self.authentication_enabled = False
        self.auth_login = None
        self.auth_password = None

    # def enable_ssl_connection(self, keystore_path, keystore_pass, truststore_path, traststore_pass):
    #     self.ssl_connection_enabled = True
    #     self.ssl_connection_string = '-ssl_enabled -ssl_key_store_path={} -ssl_key_store_password={} ' \
    #                                  '-ssl_truststore_path={} -ssl_truststore_password={}'.\
    #                                             format(keystore_path, keystore_pass, truststore_path, traststore_pass)

    def enable_ssl_connection(self, conn_tuple):
        self.ssl_connection_string = '-ssl_enabled -ssl_key_store_path={} -ssl_key_store_password={} ' \
                                     '-ssl_truststore_path={} -ssl_truststore_password={}'.format(
            conn_tuple.keystore_path, conn_tuple.keystore_pass,
            conn_tuple.truststore_path, conn_tuple.truststore_pass
        )
        self.ssl_connection_enabled = True

    @step('SU command: {*args}')
    def snapshot_utility(self, *args, **kwargs):
        """
        :param args: command and arguments
        :param kwargs:
            run_on_node - run command using particular node as --host parameter;
            background - run command in background mode
            standalone - standalone mode. there is no need cluster to be run. Used for standalone commands like:
            analyze or metadata

        :return:
        """
        log_print("Snapshot utility %s" % ' '.join(args))
        args = list(args)
        client_host = self.ignite.get_and_inc_client_host()
        bg = ''
        too_many_lines_num = 100

        if kwargs.get('standalone'):
            server_host, server_port = None, None
        else:
            server_host, server_port = self.get_any_server_node_host_port()

        if kwargs.get('run_on_node'):
            node_idx = kwargs.get('run_on_node')
            server_host = self.ignite.nodes[node_idx]['host']
            server_port = self.ignite.nodes[node_idx]['binary_rest_port']

        if kwargs.get('background'):
            file_name = kwargs.get('log', f'tmp_output_{self.get_next_run_count()}.out')
            log_file = f'{self.ignite.client_ignite_home}/work/log/{file_name}'
            log_print(f'Execute command in background mode. Output will be saved to {log_file}')
            bg = f'> {log_file} 2>&1 &'

        if self.authentication_enabled:
            if self.auth_login:
                args.append('-user=%s' % self.auth_login)
            if self.auth_password:
                args.append('-password=%s' % self.auth_password)

        if self.ssl_connection_enabled:
            args.append(self.ssl_connection_string)

        if kwargs.get('standalone'):
            # standalone mode. there is no need cluster to be run. Used for standalone commands like:
            # analyze or metadata
            commands = {
                client_host: [
                    "cd %s; bin/snapshot-utility.sh %s %s;echo Exit code: $?" % (
                        self.ignite.client_ignite_home,
                        ' '.join(args),
                        bg
                    )
                ]
            }
        else:
            commands = {
                client_host: [
                    "cd %s; bin/snapshot-utility.sh %s -host=%s -port=%s %s" % (
                        self.ignite.client_ignite_home,
                        ' '.join(args),
                        server_host,
                        server_port,
                        bg if bg else ';echo Exit code: $?'
                    )
                ]
            }

        self.ignite.logger.debug(commands)
        results = self.ignite.ssh.exec(commands, **kwargs)
        lines = results[client_host][0]
        self.latest_utility_output = lines

        if len(lines.split('\n')) > too_many_lines_num:
            lines_to_show = '\n'.join(lines.split('\n')[:too_many_lines_num]) + '\n*** TOO MANY LINES TO SHOW ***'
        else:
            lines_to_show = lines

        if 'snapshot' in str(args[0]).lower() and not kwargs.get('all_required'):
            success = self._print_snapshot_utility_output(lines, 'Snapshot created:', 'successfully finished')
            ss_id = self.get_snapshot_id(lines)

            if success:
                print_blue("Appending snapshot id: %s" % ss_id)
                self.snapshots.append({
                    'id': ss_id,
                    'type': 'FULL' if 'full' in str(' '.join(args)).lower() else 'INC'
                })
                return ss_id
            elif not kwargs.get('background'):
                raise TidenException(''.join(lines_to_show))

        elif kwargs.get('all_required'):
            self._print_snapshot_utility_output(lines, 'Command', 'successfully finished')
            success = self.check_content_all_required(lines, kwargs.get('all_required'),
                                                      maintain_order=kwargs.get('maintain_order', None),
                                                      escape=kwargs.get('escape', None))
        else:
            success = self._print_snapshot_utility_output(lines, 'Command', 'successfully finished')

        if not success and not kwargs.get('background'):
            raise TidenException(''.join(lines_to_show))

        if kwargs.get('get_output_file_content'):
            if '-output' in str(args[1]).lower():
                output_file = [item[8:] for item in str(args[1]).split() if '-output' in item.lower()][0]
                results = self.ignite.ssh.exec_on_host(client_host, ['cat %s' % output_file], **kwargs)
                buffer = results[client_host][0]
                self.output_file_content = buffer

    @staticmethod
    def check_content_all_required(buff, lines_to_search, maintain_order=False, escape=None):
        """
        This method checks the all lines in lines_to_search list could be found in buff. If not then exception
        TidenException will be risen.

        :param buff:
        :param lines_to_search:
        :return:
        """
        import re
        search_in = [line for line in buff.split('\n') if line]
        if escape:
            escape_from_search = []
            for item_to_escape in escape:
                tmp_ = [line for line in search_in if item_to_escape in line]
                escape_from_search += tmp_
            if escape_from_search:
                search_in = [item for item in search_in if item not in escape_from_search]

        search_for = list(lines_to_search)
        found = []
        result = True

        if maintain_order:
            search_left = search_for.copy()
            for line in search_in:
                if len(search_left) <= 0:
                    break
                cur_search_for = search_left[0]
                m = re.search(cur_search_for, line)
                if m:
                    found.append(cur_search_for)
                    search_left = search_left[1:]

        else:
            for line_to_search in search_for:
                for line in search_in:
                    m = re.search(line_to_search, line)
                    if m:
                        found.append(line_to_search)
                        break
        if len(search_for) != len(found):
            get_logger('tiden').debug('Searching \n%s \nand found: \n%s \nin buffer \n%s'
                                      % ('\n'.join(search_for), '\n'.join(found), '\n'.join(search_in)))
            if len(search_for) > len(found):
                raise TidenException('Searching \n%s \nand found: \n%s \nin buffer \n%s.\nCan\'t find:\n%s'
                                 % ('\n'.join(search_for),
                                    '\n'.join(found),
                                    '\n'.join(search_in),
                                    set(search_for).difference(set(found))))
            else:
                raise TidenException('Searching \n%s \nand found: \n%s \nin buffer \n%s.\nFound additional items:\n%s'
                                     % ('\n'.join(search_for),
                                        '\n'.join(found),
                                        '\n'.join(search_in),
                                        set(found).difference(set(search_for))))
        return result

    @staticmethod
    def _print_snapshot_utility_output(output, start_msg, stop_msg):
        success, include_in_log = False, False
        for line in output.split('\n'):
            if line.startswith(start_msg):
                include_in_log = True
            if include_in_log:
                log_print(line)
            if stop_msg in line:
                log_print(line)
                include_in_log = False
                if 'successfully finished' in line:
                    success = True
        return success

    def create_shared_snapshot_storage(self, unique_folder=False, prefix=None):
        if unique_folder:
            from time import time
            from inspect import stack
            caller_name = stack()[1][3]
            if prefix:
                caller_name = '%s_%s' % (prefix, caller_name)
            snapshot_storage = '%s_%s' % (caller_name, int(time()))
        else:
            snapshot_storage = 'snapshot_%s_test' % (self.ignite.config['environment']['username'])

        from pt.tidenfabric import TidenFabric
        nas_manager = TidenFabric().getNasManager()
        return nas_manager.create_shared_folder(snapshot_storage)

    def remove_shared_snapshot_storage(self, snapshot_storage):
        from pt.tidenfabric import TidenFabric
        if snapshot_storage:
            nas_manager = TidenFabric().getNasManager()
            return nas_manager.remove_shared_folder(snapshot_storage)
        else:
            print_red('Snapshot storage is None!!!')

    def get_created_snapshot_id(self, snapshot_number):
        snapshot_id = None
        if snapshot_number <= len(self.snapshots):
            snapshot_id = self.snapshots[snapshot_number - 1]['id']
        return snapshot_id

    def clear_snapshots_list(self):
        self.snapshots = []

    @staticmethod
    def get_operation_id(buffer):
        operation_id = None
        for line in buffer.split('\n'):
            m = search('Operation ID: ([0-9,a-f\-]+)', line)
            if m:
                operation_id = m.groups(1)
        return operation_id

    @staticmethod
    def get_snapshot_id(buffer):
        snapshot_id = None
        for line in buffer.split('\n'):
            m = search('ID: ([0-9]+)', line)
            if m:
                snapshot_id = m.group(1)
        return snapshot_id

    def get_last_command_exit_code(self):
        return self._search_in_buffer(self.latest_utility_output, 'Exit code: ([0-9]+)')

    def get_last_command_error_code(self):
        return self._search_in_buffer(self.latest_utility_output, 'ailed with error: ([0-9]+)')

    @staticmethod
    def _search_in_buffer(buffer, regexp):
        found = None
        for line in buffer.split('\n'):
            m = search(regexp, line)
            if m:
                found = m.group(1)
        return found

    @step('Wait for snapshot activity in cluster')
    def wait_no_snapshots_activity_in_cluster(self, retries=10):
        delay = 2
        expected = ['No snapshots activity in cluster', 'Command \[STATUS\] successfully finished in \d+ seconds.']

        for attempt in range(1, retries):
            try:
                self.snapshot_utility('status', all_required=expected)
                return self.latest_utility_output
            except TidenException as e:
                if attempt == (retries - 1):
                    raise e
                else:
                    log_print('TimeoutError (%s) due to wait inactivity in cluster. Retry attempt %s.' %
                              (str(e), str(attempt)))
                    pass
            finally:
                util_sleep_for_a_while(delay, msg='Attempt=%s' % str(attempt))

    def wait_running_operation(self):
        delay = 1
        retries = 30
        operation_id = None

        for attempt in range(1, retries):
            self.snapshot_utility('status')
            operation_id = self.get_operation_id(self.latest_utility_output)

            if operation_id:
                break
            else:
                util_sleep_for_a_while(delay, msg='Waiting operation ID. Attempt=%s' % str(attempt))
                if attempt == (retries - 1):
                    msg = 'TimeoutError due to wait operation ID in cluster. Attempt %s' % str(attempt)
                    raise TidenException(msg)
        return operation_id

    def wait_running_snapshot(self, log):
        delay = 1
        retries = 10
        snapshot_id = None

        client_host = self.ignite.get_and_inc_client_host()

        commands = {
            client_host: [
                "cat %s/%s" % (self.ignite.config['rt']['remote']['test_dir'], log)
            ]
        }

        for attempt in range(1, retries):
            self.snapshot_utility('status')
            result = self.ignite.ssh.exec_on_host(client_host, [commands])
            snapshot_id = self.get_snapshot_id(result[client_host])

            if not snapshot_id:
                util_sleep_for_a_while(delay, msg='Waiting snapshot ID. Attempt=%s' % str(attempt))
                if attempt == (retries - 1):
                    msg = 'TimeoutError due to wait snapshot ID in cluster. Attempt %s' % str(attempt)
                    raise TidenException(msg)
            else:
                break
        return snapshot_id

    def check_all_msgs_in_utility_output(self, lines_to_search):
        utility_output = self.latest_utility_output.split('\n')
        for line in lines_to_search:
            found = [item for item in utility_output if line in item]
            tiden_assert_equal(1, len(found),
                               'Str %s found in utility output: %s' % (line, '\n'.join(utility_output))
                               )

    def util_delete_scheduled_task(self, task_name, check_no_tasks=False):
        if isinstance(task_name, list):
            tasks = task_name
        else:
            tasks = [task_name]

        for name in tasks:
            task_param = '-delete -name=%s' % name
            self.snapshot_utility('schedule', task_param)

        if check_no_tasks:
            schedule_success_empty = ['No schedules found',
                                      'Command \[SCHEDULE\] successfully finished in \d+ seconds.']
            self.snapshot_utility('schedule', '-list', all_required=schedule_success_empty)

    def util_get_not_default_snapshot_location(self, folder_name='some_another_folder'):
        new_location = None
        commands = {}
        for node_id in self.ignite.nodes.keys():
            if self.ignite.is_default_node(node_id):
                host = self.ignite.nodes[node_id]['host']
                ignite_home = self.ignite.nodes[node_id]['ignite_home']
                new_location = '%s/%s' % (ignite_home, folder_name)
                commands[host] = ['mkdir %s' % new_location]

        print_red(commands)
        tmp_output = self.ignite.ssh.exec(commands)
        print_red(tmp_output)
        return new_location

    def copy_utility_log(self):
        commands = {}
        to = self.ignite.config['rt']['remote']['test_dir']
        for host in self.ignite.config['environment']['client_hosts']:
            if commands.get(host) is None:
                commands[host] = [
                    'cd %s/work/log; mv snapshot-utility.log %s/snapshot-utility-test.log;' % (
                        self.ignite.client_ignite_home, to),
                ]
        self.ignite.ssh.exec(commands)

    def get_snapshots_from_list_command(self):
        snapshots = []
        self.snapshot_utility('list')
        for line in self.latest_utility_output.split('\n'):
            m = search('ID=(\d+)', line)
            if m:
                print_green(m.group(1))
                snapshots.append(m.group(1))
        return snapshots

    @staticmethod
    def util_get_scheduled_datetime_in_cron_format(exact=False, delta=2):
        import datetime
        import time
        if exact:
            after_some_mins = datetime.datetime.now() + datetime.timedelta(minutes=delta)
            # Hack if TC docker runs in UTC time zone
            if int(time.timezone / -(60 * 60)) == 0:
                after_some_mins = after_some_mins + datetime.timedelta(hours=3)

            return after_some_mins.strftime("%M %H %d %m ") + '*'
        else:
            return '*/2 * * * *'

    def snapshots_info(self):
        repr_str = 'Snapshots info:'
        for snapshot in self.snapshots:
            repr_str += '\n ID=%s, TYPE=%s' % (snapshot.get('id'), snapshot.get('type'))

        return repr_str

    def get_any_server_node_host_port(self):
        """
        Returns server host and command port from first alive default node.
        :return:
        """
        server_host, server_port = None, None
        alive_server_nodes = self.ignite.get_alive_default_nodes()

        for node_idx in alive_server_nodes:
            server_host = self.ignite.nodes[node_idx]['host']
            if 'binary_rest_port' not in self.ignite.nodes[node_idx]:
                # this node not run or killed, skip to next node
                continue
            server_port = self.ignite.nodes[node_idx]['binary_rest_port']
            break
        if server_host is None or server_port is None:
            raise TidenException('Not found running server nodes')
        return server_host, server_port

    def get_next_run_count(self):
        self.run_count += 1
        return self.run_count
