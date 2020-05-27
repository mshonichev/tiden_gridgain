import hashlib

from .class_utils import ModelTypes
from .operation_utils import convert_to_java_array
from ..piclient import get_gateway, PiClientException


class IgniteCache:
    def __init__(self, name, config=None, gateway=None):
        self.gateway = get_gateway(gateway)

        self.name = name

        if not config:
            self.cache = self.gateway.entry_point.getIgniteService().getIgnite().getOrCreateCache(name)
        else:
            config_object = config.get_config_object()
            config_object.setName(name)
            self.cache = self.gateway.entry_point.getIgniteService().getIgnite().getOrCreateCache(config_object)

    def put(self, key, value, **kwargs):
        if kwargs.get('key_type'):
            self.gateway.entry_point.getUtilsService().getNumberUtils().putTypedKey(self.cache, key, value,
                                                                                    kwargs.get('key_type'))
        else:
            self.cache.put(key, value)

    def get(self, key, **kwargs):
        if kwargs.get('key_type'):
            return self.gateway.entry_point.getUtilsService().getNumberUtils().getTypedKey(self.cache, key,
                                                                                           kwargs.get('key_type'))
        else:
            return self.cache.get(key)

    def put_all(self, java_map):
        self.cache.putAll(java_map)

    def remove(self, key, **kwargs):
        if kwargs.get('key_type'):
            return self.gateway.entry_point.getUtilsService().getNumberUtils().removeTypedKey(self.cache, key,
                                                                                              kwargs.get('key_type'))
        else:
            return self.cache.remove(key)

    def remove_all(self, java_set):
        self.cache.removeAll(java_set)

    def size(self, *args):
        peek_size_class = self.gateway.jvm.org.apache.ignite.cache.CachePeekMode
        peek_size_array = self.gateway.new_array(peek_size_class, len(args))

        for i, arg in enumerate(args):
            if arg.upper() == 'ALL':
                peek_size_array[i] = self.gateway.jvm.org.apache.ignite.cache.CachePeekMode.ALL
            elif arg.upper() == 'NEAR':
                peek_size_array[i] = self.gateway.jvm.org.apache.ignite.cache.CachePeekMode.NEAR
            elif arg.upper() == 'PRIMARY':
                peek_size_array[i] = self.gateway.jvm.org.apache.ignite.cache.CachePeekMode.PRIMARY
            elif arg.upper() == 'BACKUP':
                peek_size_array[i] = self.gateway.jvm.org.apache.ignite.cache.CachePeekMode.BACKUP
            elif arg.upper() == 'ONHEAP':
                peek_size_array[i] = self.gateway.jvm.org.apache.ignite.cache.CachePeekMode.ONHEAP
            elif arg.upper() == 'OFFHEAP':
                peek_size_array[i] = self.gateway.jvm.org.apache.ignite.cache.CachePeekMode.OFFHEAP
            else:
                raise PiClientException("Unknown CachePeekMode")

        return self.cache.size(peek_size_array)

    def destroy(self):
        self.cache.destroy()


class IgniteCacheConfig:
    def __init__(self, gateway=None):
        self.gateway = get_gateway(gateway)

        self.cacheConfig = self.gateway.jvm.org.apache.ignite.configuration.CacheConfiguration()

    def set_plugin_configurations(self, sender_group):
        set_sender_group = self.gateway.jvm.org.gridgain.grid.cache.dr.CacheDrSenderConfiguration().setSenderGroup(sender_group)

        plugin_configuration = [self.gateway.jvm.org.gridgain.grid.configuration.GridGainCacheConfiguration().setDrSenderConfiguration(
            set_sender_group), ]

        self.cacheConfig.setPluginConfigurations(convert_to_java_array(self.gateway.jvm.org.apache.ignite.plugin.CachePluginConfiguration, plugin_configuration, self.gateway))

    def set_atomicity_mode(self, atomicity_mode='ATOMIC'):
        if atomicity_mode.upper() == 'ATOMIC':
            self.cacheConfig.setAtomicityMode(
                self.gateway.jvm.org.apache.ignite.cache
                    .CacheAtomicityMode.ATOMIC)
        elif atomicity_mode.upper() == 'TRANSACTIONAL':
            self.cacheConfig.setAtomicityMode(
                self.gateway.jvm.org.apache.ignite.cache
                    .CacheAtomicityMode.TRANSACTIONAL)
        elif atomicity_mode.upper() == 'TRANSACTIONAL_SNAPSHOT':
            self.cacheConfig.setAtomicityMode(
                self.gateway.jvm.org.apache.ignite.cache
                    .CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
        else:
            raise PiClientException("Unknown atomicity mode")

    def set_cache_mode(self, cache_mode='PARTITIONED'):
        if cache_mode.upper() == 'PARTITIONED':
            self.cacheConfig.setCacheMode(
                self.gateway.jvm.org.apache.ignite.cache
                    .CacheMode.PARTITIONED
            )
        elif cache_mode.upper() == 'REPLICATED':
            self.cacheConfig.setCacheMode(
                self.gateway.jvm.org.apache.ignite.cache
                    .CacheMode.REPLICATED
            )
        else:
            raise PiClientException("Unknown cache mode")

    def set_eviction_policy_fifo(self, max_size):
        self.cacheConfig.setEvictionPolicyFactory(
            self.gateway.jvm.org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicyFactory(max_size)
        )

    def set_eviction_policy_sorted(self, max_size):
        self.cacheConfig.setEvictionPolicyFactory(
            self.gateway.jvm.org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicyFactory(max_size)
        )

    def set_expiry_policy_factory(self, factory):
        self.cacheConfig.setExpiryPolicyFactory(
            factory
        )

    def set_partition_loss_policy(self, loss_policy):
        loss_policy_mapping = {
            'READ_ONLY_SAFE': self.gateway.jvm.org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_SAFE,
            'READ_ONLY_ALL': self.gateway.jvm.org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_ALL,
            'READ_WRITE_SAFE': self.gateway.jvm.org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE,
            'READ_WRITE_ALL': self.gateway.jvm.org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_ALL
        }

        if loss_policy_mapping.get(loss_policy.upper()):
            self.cacheConfig.setPartitionLossPolicy(loss_policy_mapping.get(loss_policy.upper()))

    def set_expiry_policy_factory_accessed(self, seconds):
        time_unit = self.gateway.jvm.java.util.concurrent.TimeUnit.SECONDS
        duration = self.gateway.jvm.javax.cache.expiry.Duration(time_unit, seconds)
        expire_policy_factory = self.gateway.jvm.javax.cache.expiry.AccessedExpiryPolicy.factoryOf(duration)
        self.set_expiry_policy_factory(expire_policy_factory)

    def set_onheap_cache_enabled(self, onheap_enabled=True):
        self.cacheConfig.setOnheapCacheEnabled(onheap_enabled)

    def set_group_name(self, name):
        self.cacheConfig.setGroupName(name)

    def set_eager_ttl(self, eager_ttl=True):
        self.cacheConfig.setEagerTtl(eager_ttl)

    def set_name(self, name):
        self.cacheConfig.setName(name)

    def set_backups(self, backups):
        self.cacheConfig.setBackups(backups)

    def set_affinity(self, excl_neighbors, parts):
        self.cacheConfig.setAffinity(
            self.gateway.jvm.org.apache.ignite.cache.affinity.rendezvous
                .RendezvousAffinityFunction(excl_neighbors, parts))

    def set_rebalance_mode(self, rebalance_mode='sync'):
        if rebalance_mode.upper() == 'SYNC':
            self.cacheConfig.setRebalanceMode(
                self.gateway.jvm.org.apache.ignite.cache
                    .CacheRebalanceMode.SYNC)
        elif rebalance_mode.upper() == 'ASYNC':
            self.cacheConfig.setRebalanceMode(
                self.gateway.jvm.org.apache.ignite.cache
                    .CacheRebalanceMode.ASYNC)
        else:
            raise PiClientException("Unknown rebalance mode: %s" % rebalance_mode)

    def set_write_synchronization_mode(self, sync_mode='FULL_SYNC'):
        if sync_mode.upper() == 'FULL_SYNC':
            self.cacheConfig.setWriteSynchronizationMode(
                self.gateway.jvm.org.apache.ignite.cache
                    .CacheWriteSynchronizationMode.FULL_SYNC)
        elif sync_mode.upper() == 'FULL_ASYNC':
            self.cacheConfig.setWriteSynchronizationMode(
                self.gateway.jvm.org.apache.ignite.cache
                    .CacheWriteSynchronizationMode.FULL_ASYNC)
        elif sync_mode.upper() == 'PRIMARY_SYNC':
            self.cacheConfig.setWriteSynchronizationMode(
                self.gateway.jvm.org.apache.ignite.cache
                    .CacheWriteSynchronizationMode.PRIMARY_SYNC)
        else:
            raise PiClientException("Unknown synchronization mode: %s" % sync_mode)

    def get_config_object(self):
        return self.cacheConfig


class IgniteTransaction:
    def __init__(self, concurrency='PESSIMISTIC', isolation='REPEATABLE_READ', timeout=0, size=10000, gateway=None, ):
        self.gateway = get_gateway(gateway)

        self.ignite = self.gateway.entry_point.getIgniteService().getIgnite()

        if concurrency.upper() == 'PESSIMISTIC':
            self.concurrency = self.gateway.jvm.org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC
        elif concurrency.upper() == 'OPTIMISTIC':
            self.concurrency = self.gateway.jvm.org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC
        else:
            raise PiClientException("Unknown transaction concurrency mode")

        if isolation.upper() == 'REPEATABLE_READ':
            self.isolation = self.gateway.jvm.org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ
        elif isolation.upper() == 'READ_COMMITTED':
            self.isolation = self.gateway.jvm.org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED
        elif isolation.upper() == 'SERIALIZABLE':
            self.isolation = self.gateway.jvm.org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE
        else:
            raise PiClientException("Unknown transaction isolation mode")

        self.timeout = timeout
        self.size = size

    def __enter__(self):
        self.transaction = self.ignite.transactions().txStart(self.concurrency, self.isolation, self.timeout, self.size)

        return self

    def __exit__(self, e_type, value, traceback):
        # todo add exception
        self.transaction.close()

    def commit(self):
        self.transaction.commit()

    def rollback(self):
        self.transaction.rollback()


class Md5Cache:
    def __init__(self):
        self.m = hashlib.md5()

    def append(self, string_value):
        self.m.update(string_value)

    def compute(self):
        return self.m.digest()


class DynamicCachesFactory:
    with_indices = False

    def __init__(self):
        self.dynamic_cache_configs = [attr for attr in dir(self) if 'create_cache_' in attr]

    @staticmethod
    def with_indexes(cache_config, gateway):
        # create indices if needed
        if DynamicCachesFactory.with_indices:
            query_indices_names = gateway.jvm.java.util.ArrayList()
            query_indices_names.add("id")
            query_indices = gateway.jvm.java.util.ArrayList()
            query_indices.add(
                gateway.jvm.org.apache.ignite.cache.QueryIndex().setFieldNames(query_indices_names, True))

            query_entities = gateway.jvm.java.util.ArrayList()
            query_entities.add(
                gateway.jvm.org.apache.ignite.cache.QueryEntity("java.lang.Long",
                                                                ModelTypes.VALUE_ACCOUNT.value)
                    .addQueryField("timeStamp", "java.lang.String", None)
                    .addQueryField("id", "java.lang.Long", None)
                    .setIndexes(query_indices)
            )

            cache_config.get_config_object().setQueryEntities(query_entities)
            cache_config.get_config_object().setStatisticsEnabled(False)

    @staticmethod
    def create_cache_atomic_partitioned_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_replicated_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_partitioned_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_replicated_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_partitioned_eviction_fifo_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_replicated_eviction_fifo_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_partitioned_eviction_fifo_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_replicated_eviction_fifo_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_partitioned_onheap_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_replicated_onheap_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_partitioned_onheap_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_replicated_onheap_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_partitioned_eviction_fifo_onheap_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_replicated_eviction_fifo_onheap_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_partitioned_eviction_fifo_onheap_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_replicated_eviction_fifo_onheap_full_sync_2(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_partitioned_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_affinity(False, 128)

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_replicated_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_affinity(False, 128)

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_partitioned_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_affinity(False, 128)

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_replicated_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_affinity(False, 128)

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_partitioned_eviction_fifo_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_affinity(False, 128)
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_replicated_eviction_fifo_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_affinity(False, 128)
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_partitioned_eviction_fifo_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_affinity(False, 128)
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_replicated_eviction_fifo_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_affinity(False, 128)
        cache_config.set_onheap_cache_enabled()

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_partitioned_onheap_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_onheap_cache_enabled()
        cache_config.set_affinity(False, 128)

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_replicated_onheap_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_onheap_cache_enabled()
        cache_config.set_affinity(False, 128)

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_partitioned_onheap_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_onheap_cache_enabled()
        cache_config.set_affinity(False, 128)

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_replicated_onheap_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_onheap_cache_enabled()
        cache_config.set_affinity(False, 128)

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_partitioned_eviction_fifo_onheap_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_onheap_cache_enabled()
        cache_config.set_affinity(False, 128)

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_atomic_replicated_eviction_fifo_onheap_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('ATOMIC')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_onheap_cache_enabled()
        cache_config.set_affinity(False, 128)

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_partitioned_eviction_fifo_onheap_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('PARTITIONED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_onheap_cache_enabled()
        cache_config.set_affinity(False, 128)

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()

    @staticmethod
    def create_cache_transactional_replicated_eviction_fifo_onheap_full_sync_2_rendezvous(cache_name, gateway=None):
        gateway = get_gateway(gateway)

        cache_config = IgniteCacheConfig(gateway)

        cache_config.set_name(cache_name)
        cache_config.set_group_name(cache_name)
        cache_config.set_atomicity_mode('TRANSACTIONAL')
        cache_config.set_cache_mode('REPLICATED')
        cache_config.set_backups(2)
        cache_config.set_write_synchronization_mode('FULL_SYNC')
        cache_config.set_eviction_policy_fifo(1000)
        cache_config.set_onheap_cache_enabled()
        cache_config.set_affinity(False, 128)

        DynamicCachesFactory.with_indexes(cache_config, gateway)

        return cache_config.get_config_object()
