
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

