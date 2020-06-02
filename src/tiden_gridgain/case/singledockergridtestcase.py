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

from ..apps.ignitedocker import Ignitedocker, NodeConfig
from .singlegridzootestcase import SingleGridZooTestCase


class SingleDockerGridTestCase(SingleGridZooTestCase):

    def __init__(self, config, ssh):
        super().__init__(config, ssh)
        if config.get('rt'):
            self.ignite: Ignitedocker = Ignitedocker(self.config, self.ssh, name='ignite')

    def setup(self):
        super().setup()

    def start_grid(self, network=None, swarm=False):
        server_hosts = self.config['environment'].get('server_hosts', [])

        # Add nodes
        node_idx = 1
        test_module_dir = self.config['rt']['remote']['test_module_dir']
        node_configs = []
        for server_host in server_hosts:
            for idx in range(0, int(self.config['environment'].get('servers_per_host', 1))):

                if swarm:
                    self.set_current_context('swarm')

                config_path = [
                    '{}/{}'.format(test_module_dir, self.get_server_config()),
                    '{}/caches.xml'.format(test_module_dir)]

                docker_env = {'cpu': 4, 'memory': '4g', 'oom_killer': True}
                if self.config['environment'].get('docker'):
                    docker_env.update(self.config['environment'].get('docker'))

                node_configs.append(
                    NodeConfig()
                        .set_configs(*config_path)
                        .set_network(network)
                        .set_cpus(docker_env.get('cpu'))
                        .set_memory(docker_env.get('memory'))
                        .set_oom_killer(docker_env.get('oom_killer'))
                        .set_server_host(server_host)
                        .set_node_order(node_idx)
                )

                node_idx += 1

        self.ignite.start(node_configs)

