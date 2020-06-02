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

"""
Generator builders to build different key generators on Java side

Note that they contains set_gateway() method so user can overwrite and build this generator object on any piclient that
he needs.

There is collision_possibility parameter for TransactionalLoading - it defines key collision possibility while selecting
next keys to modify.
"""

from ..piclient import get_gateway


class AffinityCountKeyGeneratorBuilder:
    """
    Builder for Java AffinityCountKeyGenerator
    """

    def __init__(self, cache_name, consistent_node_id, start_key, keys_count, included, gateway=None):
        self.gateway = get_gateway(gateway)

        self.cache_name = cache_name
        self.consistent_node_id = consistent_node_id
        self.start_key = start_key
        self.keys_count = keys_count
        self.included = included

        # optional
        self.collision_possibility = None

    def set_collision_possibility(self, collision_possibility):
        """
        :param collision_possibility: add collision possibility for getRandomKey() method (for TransactionalLoading)
        """
        if collision_possibility:
            self.collision_possibility = collision_possibility

        return self

    def set_gateway(self, gateway):
        """
        :param gateway: overwrite gateway if needed
        """
        self.gateway = get_gateway(gateway)

        return self

    def build(self):
        """
        :return: Java object
        """
        generator = self.gateway.jvm.org.apache.ignite.piclient.operations.generators.AffinityCountKeyGenerator(
            self.start_key, self.keys_count, self.consistent_node_id, self.cache_name, self.included)

        if self.collision_possibility:
            generator.setCollisionPossibility(self.collision_possibility)

        return generator


class AffinityPartitionKeyGeneratorBuilder:
    """
    Builder for Java AffinityPartitionKeyGenerator
    """

    def __init__(self, cache_name, parts_distribution, start_key, keys_count, gateway=None):
        self.gateway = get_gateway(gateway)

        self.cache_name = cache_name
        self.parts_distribution = parts_distribution
        self.start_key = start_key
        self.keys_count = keys_count

        # optional
        self.collision_possibility = None

    def set_collision_possibility(self, collision_possibility):
        """
        :param collision_possibility: add collision possibility for getRandomKey() method (for TransactionalLoading)
        """
        if collision_possibility:
            self.collision_possibility = collision_possibility

        return self

    def set_gateway(self, gateway):
        """
        :param gateway: overwrite gateway if needed
        """
        self.gateway = get_gateway(gateway)

        return self

    def build(self):
        """
        :return: Java object
        """
        generator = self.gateway.jvm.org.apache.ignite.piclient.operations.generators.AffinityPartitionKeyGenerator(
            self.start_key, self.keys_count, self.parts_distribution, self.cache_name)

        if self.collision_possibility:
            generator.setCollisionPossibility(self.collision_possibility)

        return generator

