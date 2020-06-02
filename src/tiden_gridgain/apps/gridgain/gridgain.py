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

from tiden.apps.ignite import Ignite
from ...utilities import GridGainControlUtility, SnapshotUtility, ReplicationUtility


class Gridgain(Ignite):

    _su = None
    _ru = None

    def get_control_utility(self):
        if self._cu is None:
            self._cu = GridGainControlUtility(self)
        return self._cu

    def get_snapshot_utility(self):
        if self._su is None:
#            from tiden.utilities.snapshot_utility import SnapshotUtility
            self._su = SnapshotUtility(self)
        return self._su

    su = property(get_snapshot_utility, None)

    def get_replication_utility(self):
        if self._ru is None:
#            from tiden.utilities.replication_utility import ReplicationUtility
            self._ru = ReplicationUtility(self)
        return self._ru

    ru = property(get_replication_utility, None)

