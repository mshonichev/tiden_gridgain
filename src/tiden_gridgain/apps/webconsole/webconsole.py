#!/usr/bin/env python3

from json import loads as loads_json
from os.path import basename
from time import time, sleep

from requests import post

from tiden.apps.app import App
from tiden.assertions import tiden_assert
from tiden.tidenexception import TidenException
from tiden.dockermanager import DockerManager
from tiden.util import log_print

"""
Web Console

1. Used web-console-standalone-x.x.x.tar.gz
2. Upload artifact (do not unzip) on host
3. Load archive as docker image
4. Run app on specified port
5. Waiting for logs and check for errors in there
6. Create new user

Web Agent 

1. Used ignite-web-agent-x.x.x.zip
2. Made configuration file with user token, web-console url and REST ignite node urls
3. Run agent 
4. Waiting for establishing connection

"""


class Webconsole(App):

    def __init__(self, name, config, ssh):
        name = 'Web Console' if name is None else name
        App.__init__(self, name, config, ssh)
        self.docker = DockerManager(config, ssh)
        self.image_name = 'gridgain/web-console-standalone'
        self.image_tag = None

        self.base_headers = {
            "Content-Type": "application/json",
            'Accept': 'application/json'
        }

    @property
    def last_node_id(self):
        return len(self.nodes) - 1

    @property
    def first_user(self):
        if self.last_node_id in self.nodes and self.nodes[self.last_node_id].get('users'):
            return list(self.nodes[self.last_node_id]['users'].values())[0]
        return None

    def check_requirements(self):
        self.require_artifact('image_dump')
        running_containers = self.docker.get_running_containers(predicate=lambda x: '80/tcp' in x.get('port'))
        if running_containers:
            raise TidenException('Found running Web Console container on hosts: {}\nDetails:\n{}'.
                                 format(list(running_containers.keys()), running_containers))

    def teardown(self):
        self.stop()

    def set_image_info(self, **kwargs):
        if kwargs.get('image_name'):
            self.image_name = kwargs.get('image_name')
        if kwargs.get('image_tag'):
            self.image_tag = kwargs.get('image_tag')

    def start(self, **kwargs):
        """
        Run web console and create user if it needed

        :param image_name:      web-console image name
        :param image_tag:       web-console image tag
        :param host:            host ip where need to start app (or run on first found host)
        :param port:            web-console port which will be entering point for UI and REST API
        :param users:           create user after start:
                                    bool: users:
                                        True - create user with default data
                                        False, None - do not create user
                                    list, tuple or something iterable:
                                        going through list users data and create each one
        """
        port = kwargs.get('port', 80)
        if kwargs.get('image_name'):
            self.image_name = kwargs.get('image_name')

        if kwargs.get('image_tag'):
            self.image_tag = kwargs.get('image_tag')
        else:
            self.image_tag = self.get_image_tag_from_artifact()

        if not kwargs.get('host'):
            # pick the first client host from config
            hosts = self.docker.check_docker_on_hosts(self.config.get('environment').get('client_hosts'))
            if len(hosts) > 0:
                host = hosts[0]
            else:
                TidenException('Could not find host to deploy docker. Hosts checked {}'.
                               format(self.config.get('environment').get('client_hosts')))

        log_print('Going to start Web Console with parameters: image_name={}, image_tag={} on host={}'.
                  format(self.image_name, self.image_tag, host), color='debug')
        self.docker.load_images()
        container_id, log_path, container_name = self.docker.run(image_name=self.image_name,
                                                                 host=host,
                                                                 ports=[(port, 80)],
                                                                 tag=self.image_tag)
        self.nodes[len(self.nodes)] = {
            'container_id': container_id,
            'log': log_path,
            'container_name': container_name,
            'host': host,
            'port': port
        }

        self.wait_to_start()
        self.wait_for_index_build()

        self.check_errors_in_logs()
        log_print('Web Console started', color='debug')

        if kwargs.get('users', False):
            users = kwargs.get('users')
            from collections.abc import Iterable

            if isinstance(users, bool):
                self.sign_up_user({})
            elif isinstance(users, Iterable):
                for user in users:
                    self.sign_up_user(user)
            else:
                TidenException('Expecting users is Boolean or Iterable, but got {}'.format(users))

    def start_web_agent(self, user_info=None, node_id=None, ignite_urls: list = None):
        """
        Create agent configuration file and start web agent with it

        :param user_info:       dict: run agent with this user token (if None then use first found user for this node)
        :param node_id:         web console node id (if None take last created)
        :param ignite_urls:     ignite rest urls to communicate with web-console (http://1.1.1.1:8080)
        """
        if node_id is None:
            node_id = self.last_node_id
        if user_info is None:
            user_info = self.first_user

        token = user_info.get('token', None)
        if token is None:
            token = self.get_user_info(node_id, user_info)['token']

        self._start_web_agent_process(node_id, token, ignite_urls)

    def _start_web_agent_process(self, node_id, token, ignite_urls=None):
        """
        Find agent home
        Create properties file
        Run agent with this file
        Waiting for logs

        :param node_id:         console node id
        :param token:           user token
        :param ignite_urls:     ignites urls
        """
        host, port = self.get_host_port(node_id)
        test_dir = self.config['rt']['remote']['test_dir']
        prop_name = 'default.properties'

        web_agent_log = '{}/web-agent.log'.format(test_dir)
        properties = {
            "tokens": token,
            'server-uri': 'http://{}:{}'.format(host, port),
        }

        agent_home = self.nodes[node_id].get('agent_home_dir', None)

        if ignite_urls:
            properties['node-uri'] = ','.join(['http://{}'.format(url) for url in ignite_urls])

        if agent_home is None:
            found_agent = [artifact for artifact in self.config['artifacts'].values()
                           if artifact.get('type') == 'web_agent']
            tiden_assert(len(found_agent) > 0, 'Failed to find web agent artifact')
            found_agent = found_agent[0]
            agent_home = '{}/{}'.format(
                found_agent['remote_path'],
                basename(found_agent['path']).replace('.zip', '').replace('.tar', '').replace('.gz', '')
            )
            self.nodes[node_id]['agent_home_dir'] = agent_home

        self.nodes[node_id]['agent_properties'] = properties

        properties = '\n'.join(['{}={}'.format(k, v) for k, v in properties.items()])
        self.ssh.exec_on_host(host, ["printf '{}\n' > {}/{}".format(properties, agent_home, prop_name)])

        run_cmd = 'nohup {}/ignite-web-agent.sh -c {}/{} > {} 2>&1 &'.\
            format(agent_home, agent_home, prop_name, web_agent_log)
        self.ssh.exec_on_host(host, [run_cmd])
        self.wait_for_message_in_log(node_id, 'Connection established', web_agent_log)

    def kill_all_agents(self):
        for host in self.ssh.hosts:
            self.ssh.exec_on_host(host,
                                  ["ps aux | grep ignite-web-agent | grep java | awk '{print \$2}' | xargs kill -f"])

    def get_host_port(self, node_id):
        return self.nodes[node_id]['host'], self.nodes[node_id]['port']

    def get_user_info(self, node_id, user_info):
        """
        Get user info from web console REST API

        :param node_id:     web-console node id
        :param user_info:   user base info
        :return:            found data
        """
        if node_id is None:
            node_id = self.last_node_id
        if user_info is None:
            user_info = self.first_user

        host, port = self.get_host_port(node_id)

        user = {
            'email': user_info['email'],
            'password': user_info['password']
        }

        sign_in_res = post('{}:{}/api/v1/signin'.format(host, port), headers=self.base_headers, json=user)
        cookies = dict(sign_in_res.cookies)
        user_res = post('{}:{}/api/v1/user'.format(host, port), headers=self.base_headers, cookies=cookies)

        user = self.nodes[node_id]['users'][user_info['email']]
        user.update(loads_json(user_res.content))
        self.nodes[node_id]['users'][user_info['email']] = user
        return user

    def sign_up_user(self, user_info, node_id=None):
        """
        Create new user

        :param user_info:   user info
        :param node_id:     web-console node id
        """
        if node_id is None:
            node_id = self.last_node_id

        user = {
            "email": 'test_account@test.test',
            "password": 'testtest1234',
            "firstName": "TidenName",
            "lastName": "TidenLastName",
            "company": "GridGain",
            "country": "Canada",
            "industry": "Banking"
        }

        user.update(user_info)

        email = user["email"]
        host, port = self.get_host_port(node_id)

        log_print('Creating user {}'.format(email), color='debug')
        signup_res = None
        for attempt in range(1, 20):
            signup_res = post('http://{}:{}/api/v1/signup'.format(host, port), headers=self.base_headers, json=user)
            if signup_res.status_code == 404:
                log_print('Failed to create user (attempt #{})'.format(attempt), color='red')
                sleep(1)
            else:
                break

        log_print('Created user:\nemail:{}\npassword:{}'.format(user.get('email'), user.get('password')))
        tiden_assert(signup_res.status_code == 200, 'Failed to create user: {}\n{}'.format(email, signup_res.text))
        registered_users = self.nodes[node_id].get('users', {})
        user.update(loads_json(signup_res.content))
        registered_users.update({email: user})
        self.nodes[node_id]['users'] = registered_users

    def check_errors_in_logs(self, node_id=None):
        """
        Trying to find some errors or fails in console log
        :param node_id:     web-console node id
        """
        if node_id is None:
            node_id = self.last_node_id

        condition_base = {
            'local_regex': '(.*)',
            'remote_grep_options': '--ignore-case',
            'get_all_found': True,
            'ignore_multiline': True
        }
        condition_base.update(remote_regex='error')
        error_condition = condition_base.copy()
        condition_base.update(remote_regex='fail')
        failed_condition = condition_base.copy()
        found_errors = self.grep_log(node_id, failed=failed_condition, error=error_condition)
        if found_errors:
            errors = []
            for found_lines in found_errors[node_id].values():
                for line in found_lines:
                    if line != '':
                        errors.append(''.join(line))
            tiden_assert(errors == [], 'Found errors:\n{}'.format('\n'.join(errors)))

    def wait_for_index_build(self, node_id=None, timeout=30, interval=1):
        if node_id is None:
            node_id = self.last_node_id
        self.wait_for(node_id, 'build index done', timeout, interval)

    def wait_to_start(self, node_id=None, timeout=30, interval=1):
        if node_id is None:
            node_id = self.last_node_id
        self.wait_for(node_id, 'Successfully daemonized', timeout, interval)

    def wait_for(self, node_id, searched_pattern, timeout=30, interval=1):
        get_logs = lambda: self.grep_log(node_id, node={
            'remote_regex': '.*{}.*'.format(searched_pattern),
            'local_regex': '.*({}).*'.format(searched_pattern)
        })

        end_time = time() + timeout
        while time() < end_time:
            logs_result = get_logs()
            if len(logs_result) > 0 and logs_result[node_id]['node'] == searched_pattern:
                log_print('Found "{}"'.format(searched_pattern), color='debug')
                return True
            sleep(interval)
        tiden_assert(False, 'Failed to find "{}" in console logs'.format(searched_pattern))

    def wait_for_message_in_log(self, node_id, msg, log_file, timeout=30, interval=1):
        cmd = ['grep \'{}\' {}'.format(msg, log_file)]

        if self.nodes.get(node_id):
            host = self.nodes[node_id]['host']

        end_time = time() + timeout
        while time() < end_time:
            logs_result = self.ssh.exec_on_host(host, cmd)
            if msg in logs_result[host][0]:
                return True
            sleep(interval)
        tiden_assert(False, 'Failed to find "{}" in logs file {}'.format(msg, log_file))

    def stop(self):
        """
        Remove all created containers
        Remove all images with same name (exclude tag name -> removed all versions of found images)
        Kill all found agent processes on each node
        """
        for node in self.nodes.values():
            self.remove_container(node)
            self.remove_image(node)
        self.kill_all_agents()

    def remove_image(self, node):
        self.docker.remove_images(host=node['host'], name=self.image_name)

    def remove_container(self, node):
        self.docker.remove_containers(host=node['host'], name=node['container_name'])

    def get_image_tag_from_artifact(self):
        """
        Extracting version from artifact's glob_path
        :return:
        """
        from re import match
        tag = None
        version_regexp = '.*-(\d[\d,\.,\-,p,b]+)\.tar\.gz'
        webconsole_artifact = self.config['artifacts'].get('webconsole')
        if webconsole_artifact and webconsole_artifact.get('glob_path'):
            m = match(version_regexp, webconsole_artifact.get('glob_path'))
            if m:
                tag = m.group(1)

        return tag
