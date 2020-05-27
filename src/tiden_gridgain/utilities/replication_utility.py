from re import search
from time import sleep

from tiden import log_print, TidenException, get_logger, print_red, print_blue, print_green, datetime, tiden_assert_equal


class ReplicationUtility:

    def __init__(self, ignite):
        self.ignite = ignite
        self.latest_utility_output = None
        self.authentication_enabled = False
        self.auth_login = None
        self.auth_password = None

    def enable_authentication(self, login, password):
        self.authentication_enabled = True
        self.auth_login = login
        self.auth_password = password

    def disable_authentication(self):
        self.authentication_enabled = False
        self.auth_login = None
        self.auth_password = None

    def replication_utility(self, *args, **kwargs):
        log_print("Replication utility %s" % ' '.join(args))
        args = list(args)
        client_host = self.ignite.get_and_inc_client_host()
        server_host = None
        server_port = None
        bg = ''

        alive_server_nodes = self.ignite.get_alive_default_nodes()
        for node_idx in self.ignite.nodes.keys():
            if node_idx in alive_server_nodes:
                server_host = self.ignite.nodes[node_idx]['host']
                if 'binary_rest_port' not in self.ignite.nodes[node_idx]:
                    # this node not run or killed, skip to next node
                    continue
                server_port = self.ignite.nodes[node_idx]['binary_rest_port']
                break
        if server_host is None or server_port is None:
            raise TidenException('Not found running server nodes')

        if kwargs.get('background'):
            print('In background mode')
            bg = '>%s 2>&1 &' % kwargs.get('log')

        if self.authentication_enabled:
            if self.auth_login:
                args.append('-user=%s' % self.auth_login)
            if self.auth_password:
                args.append('-password=%s' % self.auth_password)

        commands = {
            client_host: [
                "cd %s; bin/replication-utility.sh %s -host=%s -port=%s %s" % (
                    self.ignite.client_ignite_home,
                    ' '.join(args),
                    server_host,
                    server_port,
                    bg
                )
            ]
        }

        self.ignite.logger.debug(commands)
        results = self.ignite.ssh.exec(commands, **kwargs)
        lines = results[client_host][0]
        self.latest_utility_output = lines

        if kwargs.get('all_required'):
            self._print_replication_utility_output(lines, 'Command', 'successfully finished')
            success = self.check_content_all_required(lines, kwargs.get('all_required'),
                                                      escape=kwargs.get('escape', None))
        else:
            success = self._print_replication_utility_output(lines, 'Command', 'successfully finished')

        if not success and not kwargs.get('background'):
            raise TidenException(''.join(lines))

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
        if len(search_for) != len(found):
            get_logger('tiden').debug('Searching \n%s \nand found: \n%s \nin buffer \n%s'
                                      % ('\n'.join(search_for), '\n'.join(found), '\n'.join(search_in)))
            raise TidenException('Searching \n%s \nand found: \n%s \nin buffer \n%s'
                                 % ('\n'.join(search_for), '\n'.join(found), '\n'.join(search_in)))
        return result

    @staticmethod
    def _print_replication_utility_output(output, start_msg, stop_msg):
        success, include_in_log = False, False
        for line in output.split('\n'):
            if line.startswith(start_msg):
                include_in_log = True
            if include_in_log:
                log_print(line)
            # if line.startswith(stop_msg):
            if stop_msg in line:
                log_print(line)
                include_in_log = False
                if 'successfully finished' in line:
                    success = True
        return success

    def get_operation_id(self, buffer):
        operation_id = None
        for line in buffer.split('\n'):
            m = search('Operation ID: ([0-9,a-f\-]+)', line)
            if m:
                operation_id = m.groups(1)
        return operation_id

    def get_session_id_from_bootstrap_command(self):
        session_id = ''
        for line in self.latest_utility_output.split('\n'):
            m = search('sessionId: (\d+)', line)
            if m:
                print_green('Session ID found -  %s' % m.group(1))
                session_id = m.group(1)
        return session_id

    def get_last_applied_cut_from_session(self) -> int:
        session_id = ''
        for line in self.latest_utility_output.split('\n'):
            m = search('lastSuccessfullyAppliedCutId: (\d+)', line)
            if m:
                print_green('Session ID found -  %s' % m.group(1))
                session_id = m.group(1)
        return session_id

    def check_all_msgs_in_utility_output(self, lines_to_search):
        utility_output = self.latest_utility_output.split('\n')
        for line in lines_to_search:
            found = [item for item in utility_output if line in item]
            tiden_assert_equal(1, len(found),
                               'Str %s found in utility output: %s' % (line, '\n'.join(utility_output))
                               )

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

    @staticmethod
    def util_sleep_for_a_while(period):
        log_print(f'sleep {period} sec. started at {datetime.now().strftime("%H:%M %B %d")}')
        sleep(int(period))
