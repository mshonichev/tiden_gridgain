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

from time import time, sleep

from tiden.sshpool import SshPool
from tiden.dockermanager import DockerManager


class DockerMixin:
    """
    Common actions for all docker apps implemented this mixin
    """
    docker: DockerManager = None

    def __init__(self):
        self.docker: DockerManager = None
        self.ssh: SshPool = None
        self.nodes = {}

    def disconnect_network(self, node_id, network_name):
        self.docker.network_disconnect(host=self.nodes[node_id]['host'],
                                       container_name=self.nodes[node_id]['container_id'],
                                       network_name=network_name)

    def node_stop(self, node_id):
        self.docker.stop(host=self.nodes[node_id]['host'], name=self.nodes[node_id]['container_id'])

    def node_start(self, node_id):
        self.docker.start(host=self.nodes[node_id]['host'], name=self.nodes[node_id]['container_id'])

    def node_pause(self, node_id):
        self.docker.pause(host=self.nodes[node_id]['host'], name=self.nodes[node_id]['container_id'])

    def node_unpause(self, node_id):
        self.docker.unpause(host=self.nodes[node_id]['host'], name=self.nodes[node_id]['container_id'])

    def wait_for(self, condition, action=lambda: None, timeout=30, interval=1, failed=None):
        end_time = time() + timeout
        while True:
            result = action()
            if condition(result):
                return True
            elif failed is not None and failed(result):
                return False

            if time() > end_time:
                return False
            sleep(interval)