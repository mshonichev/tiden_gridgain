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

import random

from ..piclient import get_gateway
from enum import Enum


class ModelTypes(Enum):
    # value types
    VALUE_ACCOUNT = 'org.apache.ignite.piclient.model.values.Account'
    VALUE_ALL_TYPES = 'org.apache.ignite.piclient.model.values.AllTypes'
    VALUE_ALL_TYPES_MAPPED = 'org.apache.ignite.piclient.model.values.AllTypesMapped'
    VALUE_ALL_TYPES_MASSIVE = 'org.apache.ignite.piclient.model.values.AllTypesMassive'
    VALUE_DEFAULT_TABLE = 'org.apache.ignite.piclient.model.values.DefaultTable'
    VALUE_JOIN_TYPES = 'org.apache.ignite.piclient.model.values.JoinTypes'

    VALUE_ALL_TYPES_INDEXED = 'org.apache.ignite.piclient.model.values.AllTypesIndexed'
    VALUE_ALL_TYPES_4_INDEX = 'org.apache.ignite.piclient.model.values.AllTypes4Index'
    VALUE_ALL_TYPES_10_INDEX = 'org.apache.ignite.piclient.model.values.AllTypes10Index'
    VALUE_ALL_TYPES_30_INDEX = 'org.apache.ignite.piclient.model.values.AllTypes30Index'
    VALUE_EXT_ALL_TYPES_30_INDEX = 'org.apache.ignite.piclient.model.values.ExtAllTypes30Index'

    VALUE_ORGANIZATION = 'org.apache.ignite.piclient.model.values.indexed.Organization'
    VALUE_ORGANIZATION_SINGLE_INDEX = 'org.apache.ignite.piclient.model.values.indexed.OrganizationSingleIndex'
    VALUE_PERSON = 'org.apache.ignite.piclient.model.values.indexed.Person'

    # key types (to avoid common balance field in indexedField because of all values are Transferable)
    KEY_ACCOUNT = 'org.apache.ignite.piclient.model.keys.Account'
    KEY_ALL_TYPES = 'org.apache.ignite.piclient.model.keys.AllTypes'
    KEY_ALL_TYPES_MAPPED = 'org.apache.ignite.piclient.model.keys.AllTypesMapped'
    KEY_ALL_TYPES_MASSIVE = 'org.apache.ignite.piclient.model.keys.AllTypesMassive'
    KEY_DEFAULT_TABLE = 'org.apache.ignite.piclient.model.keys.DefaultTable'
    KEY_JOIN_TYPES = 'org.apache.ignite.piclient.model.keys.JoinTypes'

    KEY_ALL_TYPES_INDEXED = 'org.apache.ignite.piclient.model.keys.AllTypesIndexed'
    KEY_ALL_TYPES_COMPLEX_INDEX = 'org.apache.ignite.piclient.model.keys.AllTypesComplexIndex'

    @classmethod
    def random(cls):
        return random.choice([
            ModelTypes.VALUE_ACCOUNT.value,
            ModelTypes.VALUE_ALL_TYPES.value,
            ModelTypes.VALUE_ALL_TYPES_INDEXED.value,
            ModelTypes.VALUE_DEFAULT_TABLE.value,
            ModelTypes.VALUE_JOIN_TYPES.value,
            ModelTypes.VALUE_ORGANIZATION.value,
            ModelTypes.VALUE_ORGANIZATION_SINGLE_INDEX.value,
            ModelTypes.VALUE_PERSON.value,
        ])


def register_classpath(path, gateway=None):
    get_gateway(gateway).entry_point.getIgniteService().registerClasspath(path)


def get_class(class_name, gateway=None):
    return get_gateway(gateway).jvm.Class.forName(class_name)


def create_java_object(class_name, *args, gateway=None):
    gateway = get_gateway(gateway)

    object_class = gateway.jvm.Object
    object_array = gateway.new_array(object_class, len(args))

    for i, arg in enumerate(args):
        object_array[i] = arg

    return gateway.entry_point.getIgniteService().createObjectInstance(class_name, object_array)


def create_java_array(gateway=None):
    return get_gateway(gateway).jvm.java.util.ArrayList()


def create_java_map(gateway=None):
    return get_gateway(gateway).jvm.java.util.HashMap()


def create_java_set(gateway=None):
    return get_gateway(gateway).jvm.java.util.HashSet()

# Wrapped AllTypes constructor

def create_all_types(long_value, string_value=None, gateway=None):
    if not string_value:
        string_value = str(long_value)

    return get_gateway(gateway).jvm.org.apache.ignite.piclient.model.values.AllTypes(long_value, string_value)

