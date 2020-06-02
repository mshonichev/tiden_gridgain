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


class NodeConfig:

    def __init__(self):
        self.configs = []
        self.ports = []
        self.network = 'host'
        self.cpus = None
        self.memory = None
        self.oom = None
        self.server_host = None
        self.node_order_number = None

    def set_configs(self, *configs):
        self.configs = configs
        return self

    def set_ports(self, **kwargs):
        self.ports = {**self.ports, **kwargs}
        return self

    def set_network(self, name):
        self.network = name
        return self

    def set_cpus(self, count):
        self.cpus = f'"{count}"'
        return self

    def set_memory(self, memory):
        self.memory = memory
        return self

    def set_oom_killer(self, value):
        self.oom = value
        return self

    def set_server_host(self, value):
        self.server_host = value
        return self

    def set_node_order(self, value):
        self.node_order_number = value
        return self

    def get_container_limits(self):
        ekw_args = {}
        params = []

        if self.cpus:
            ekw_args['cpus'] = self.cpus
        if self.memory:
            ekw_args['memory'] = self.memory
        if self.oom is not None and not self.oom:
            params.append('-oom-kill-disable')
        return ekw_args, params

    def get_service_limits(self):
        ekw_args = {}

        if self.cpus:
            ekw_args['limit-cpu'] = self.cpus
        if self.memory:
            ekw_args['limit-memory'] = self.memory
        return ekw_args