"""
Operation wrappers for piclient

Some code cannot be created using py4j in python. Therefore following operation was created.
Note that usually operation uses cache with Long keys, so it's impossible to use following statement correctly:
cache.get(1) - 1 will be automatically interpreted into Java side as Integer value.
"""
import collections
from enum import Enum

from .generator_utils import AffinityCountKeyGeneratorBuilder, AffinityPartitionKeyGeneratorBuilder
from ..piclient import get_gateway

# Description of Ignite Transaction (concurrency, isolation, timeout, size and optional label)
TxDescriptor = collections.namedtuple('TxDescriptor',
                                      'concurrency isolation timeout size label',
                                      defaults=('PESSIMISTIC', 'REPEATABLE_READ', 0, 10000, None))


# Operation class names from piclient java part
class Operation(Enum):
    affinity_operation = "AffinityOperation"
    async_operation = "ASyncOperation"
    broadcast_message_operation = "BroadcastMessageOperation"
    broke_data_entry_operation = "BrokeDataEntryOperation"
    create_change_entry_write_version_or_value_operation = "ChangeEntryWriteVersionOrValueOperation"
    clear_entry_locally_operation = "ClearEntryLocallyOperation"
    checksum_operation = "ChecksumOperation"
    cq_operation = "ContinuousQueryOperation"
    cpu_load_operation = "CpuLoadOperation"
    dbt_atomic_long_operation = "DistributedAtomicLongOperation"
    dbt_checksum_operation = "DistributedChecksumOperation"
    event_collect_operation = "EventCollectOperation"
    event_pending_operation = "EventPendingOperation"
    jmx_ports_operation = "GetJmxPortsOperation"
    kill_on_cp_operation = "KillOnCheckpointOperation"
    multicache_transfer_operation = "MultiCacheTransferTaskOperation"
    put_all_operation = "PutAllOperation"
    put_operation = "PutOperation"
    put_with_optional_remove_operation = "PutWithOptionalRemoveOperation"
    remove_operation = "RemoveOperation"
    shuffle_binary_operation = "ShuffleBinaryOperation"
    single_cache_transfer_operation = "SingleCacheTransferTaskOperation"
    start_dynamic_caches_operation = "StartDynamicCachesOperation"
    streamer_operation = "StreamerOperation"
    sum_operation = "SumOperation"
    tx_control_operation = "TransactionControlOperation"
    tx_put_operation = "TxPutOperation"
    wal_statistics_operation = "WalRecordsStatisticsCollector"
    starvation_in_fork_join_pool = "ForkJoinPoolStarvationOperation"
    throw_custom_error_operation = "ThrowCustomErrorOperation"
    create_tables_operation = "org.apache.ignite.piclient.operations.impl.combination.setup.CreateTablesOperation"
    create_indexes_operation = "org.apache.ignite.piclient.operations.impl.combination.setup.CreateIndexesOperation"
    wait_snapshot_end_operation = "org.apache.ignite.piclient.operations.impl.combination.setup.WaitForSnapshotsOperation"


def create_async_operation(operation_method, *args, gateway=None, **kwargs):
    """
    Wrap operation to AsyncOperation
    Evaluating will be started as soon as threads resource will be available on client
    (Default piclient started with cores/2 thread available)

    :param operation_method: operation method to async
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    return gateway.entry_point.getOperationsService() \
        .createAsyncOperation(operation_method(*args, **kwargs, gateway=gateway))


def create_combine_operation(class_path, gateway=None):
    """
    Run combine operations in org.apache.ignite.piclient.operations.impl.combination
    Endless operation, running even after exception

    :param class_path:      full class path
    :param gateway:         optional gateway (default taken from threading.current_thread)
    :return:                operation
    """
    gateway = get_gateway(gateway)
    operation = _create_operation(class_path, gateway=gateway)
    return operation


def create_tables_operation(tables_count, values_count, gateway=None):
    """
    Create tables operation
    Custom count of tables
    columns (intcol integer, numbcol number, varchcol varchar, varch2col varchar2, primary key (intcol, numbcol))

    :param tables_count:    tables count to create
    :param values_count:    values count inserted in each table
    :param gateway:         optional gateway (default taken from threading.current_thread)
    :return:                operation
    """
    gateway = get_gateway(gateway)
    operation = _create_operation(Operation.create_tables_operation.value,
                                  tables_count,
                                  values_count,
                                  gateway=gateway)
    return operation


def create_indexes_operation(count, gateway=None):
    """
    Create indexes in all existed tables

    :param count:           amount of indexes to create
    :param gateway:         optional gateway (default taken from threading.current_thread)
    :return:                operation
    """
    gateway = get_gateway(gateway)
    operation = _create_operation(Operation.create_indexes_operation.value,
                                  count,
                                  gateway=gateway)
    return operation


def wait_snapshot_operation(wait_time, gateway=None):
    """
    Wait until some snapshot operation will end
    Might be snapshot creation or snapshot restore

    :param wait_time:   time to wait
    :param gateway:     optional gateway (default taken from threading.current_thread)
    :return:            operation
    """
    gateway = get_gateway(gateway)
    operation = _create_operation(Operation.wait_snapshot_end_operation.value,
                                  wait_time,
                                  gateway=gateway)
    return operation


def create_event_collect_operation(events,
                                   with_exception=None,
                                   gateway=None):
    """
    Start operation that will collect all events and trace them into log

    Returns Collection<Map<String, String>> for each node there will be Map<node_UUID, event>
    Should be merged manually at this time

    :param events: events to collect (org.apache.ignite.IgniteEvents)
    :param with_exception: with exception?
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.event_collect_operation.value,
                                  convert_to_java_array(gateway.jvm.java.lang.Integer, events, gateway),
                                  gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    return operation


def create_event_pending_operation(events,
                                   with_exception=None,
                                   gateway=None):
    """
    Start listener that will release execution flow when events happens

    :param events: events to wat (org.apache.ignite.IgniteEvents)
    :param with_exception: with exception?
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.event_pending_operation.value,
                                  convert_to_java_array(gateway.jvm.java.lang.Integer, events, gateway),
                                  gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    return operation


def create_put_with_optional_remove_operation(cache_name, start, end, remove_probability,
                                              node_consistent_id=None,
                                              tx_description=None,
                                              use_monotonic_value=None,
                                              monotonic_value_seed=None,
                                              value_type=None,
                                              with_exception=None,
                                              gateway=None):
    """
    Create may be put operation (put keys from start to end deleting some keys during put with deleteProb probability)

    :param with_exception: throw exception o execution or just print stacktrace
    :param value_type: cache value type
    :param cache_name: cache name
    :param start: start key
    :param end: end key
    :param tx_description: TxDescriptor allowed transactions
    :param use_monotonic_value
        False, put values will be equal to key (as usual for other operations),
        True, put values will be monotonically increasing starting from monotonic_value_seed.
        That helps catch key updates reordering issues.
    :param monotonic_value_seed
    :param remove_probability: delete inserted keys probability
    :param node_consistent_id: put all primary keys to node with given consistent id
    :return: number of keys inserted minus number of keys deleted during insertion
    :param gateway: optional gateway (default taken from threading.current_thread)
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.put_with_optional_remove_operation.value,
                                  cache_name, start, end, remove_probability, gateway=gateway)

    if node_consistent_id:
        operation.setKeyGenerator(gateway.jvm.org.apache.ignite.piclient.operations.generators.AffinityKeyGenerator(
            start,
            end,
            node_consistent_id,
            cache_name
        ))

    # there may be no transaction
    if tx_description:
        operation.setWithTransaction()

        _add_allowed_transactions_to_loading(operation, (tx_description,), gateway)

    if use_monotonic_value is not None:
        operation.setUseMonotonicValue(use_monotonic_value)

    if monotonic_value_seed is not None:
        operation.setMonotonicValueSeed(monotonic_value_seed)

    if value_type is not None:
        operation.setValueType(value_type)

    if with_exception is not None:
        operation.setWithException(with_exception)

    if value_type is not None:
        operation.setValueType(value_type)

    return operation


def create_cpu_load_operation(load_factor,
                              num_cores,
                              thread_per_core,
                              gateway=None):
    """
    Provide CPU load on each server node in cluster

    :param load_factor: how cpu % should be used
    :param num_cores: how many % cores should be used
    :param thread_per_core: threads per core
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.cpu_load_operation.value,
                                  load_factor, num_cores, thread_per_core, gateway=gateway)

    return operation


def create_affinity_operation(with_exception=None,
                              gateway=None):
    """
    Take affinity definition for each node
    :param with_exception: throw exception o execution or just print stacktrace
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.affinity_operation.value, gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    return operation


def create_kill_on_checkpoint_operation(node_id,
                                        sleep,
                                        with_exception=None,
                                        gateway=None):
    """
    Take affinity definition for each node
    :param node_id: node id to kill
    :param with_exception: throw exception o execution or just print stacktrace
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.kill_on_cp_operation.value, node_id, sleep, gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    return operation


def create_wal_records_operation(with_exception=None,
                                 gateway=None):
    """
    Take affinity definition for each node
    :param with_exception: throw exception o execution or just print stacktrace
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.wal_statistics_operation.value, gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    return operation


def create_distributed_atomic_long(with_exception=None,
                                   gateway=None):
    """
    Take affinity definition for each node
    :param with_exception: throw exception o execution or just print stacktrace
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.dbt_atomic_long_operation.value, gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    return operation


def create_message_operation(message,
                             with_exception=None,
                             gateway=None):
    """
    Create message in each nodes log

    :param message to broadcast
    :param with_exception: throw exception o execution or just print stacktrace
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.broadcast_message_operation.value, message, gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    return operation


def create_distributed_checksum_operation(with_exception=None,
                                          gateway=None):
    """
    Operation that calculate checksum over cache values

    :param with_exception: throw exception o execution or just print stacktrace
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.dbt_checksum_operation.value, gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    return operation


def start_dynamic_caches_operation(filepath=None, batch_size=500, with_exception=None,
                                   gateway=None):
    """
    Operation that start caches list dynamic

    :param filepath: caches.xml path
    :param batch_size: batch to create
    :param with_exception: throw exception o execution or just print stacktrace
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.start_dynamic_caches_operation.value, filepath, batch_size,
                                  gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    return operation


def create_continuous_query_operation(cache_name, duration,
                                      scan_query=None, entry_update_listener=None, entry_filter_factory=None,
                                      with_exception=None,
                                      gateway=None):
    """
    Operation that start caches list dynamic

    :param cache_name: cache name for ContinuousQuery
    :param duration: duration of running ContinuousQuery

    :param scan_query: default IgniteBiPredicatePositiveKey
    :param entry_update_listener: default CacheEntryUpdatedListenerLogUpdate
    :param entry_filter_factory: default CacheEntryEventFilterFactoryPositiveKey

    :param with_exception: throw exception o execution or just print stacktrace
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.cq_operation.value, cache_name, duration, gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    if scan_query:
        operation.setScanQuery(scan_query)

    if entry_update_listener:
        operation.setEntryUpdatedListener(entry_update_listener)

    if entry_filter_factory:
        operation.setEventFilterFactory(entry_filter_factory)

    return operation


def create_checksum_operation(cache_name,
                              start=None, end=None, step=None,
                              with_exception=None,
                              gateway=None):
    """
    Operation that calculate checksum over cache values

    :param cache_name: cache name to calculate checksum
    :param start: start key
    :param end: end key
    :param step: step
    :param with_exception: throw exception o execution or just print stacktrace
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.checksum_operation.value, cache_name, gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    if start:
        operation.setStartKey(start)

    if end:
        operation.setEndKey(end)

    if step:
        operation.setStep(step)

    return operation


def create_remove_operation(cache_name, start, end,
                            with_exception=None,
                            key_type=None,
                            gateway=None):
    """
    Create streamer operation

    It puts values into defined cache name and use Long key.

    :param with_exception: throw exception o execution or just print stacktrace
    :param cache_name: cache name
    :param start: start key
    :param end: end key
    :param key_type: key type (default Long, optional - see org.apache.ignite.piclient.model entries)
    e.g. 'org.apache.ignite.piclient.model.AllTypes'
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.remove_operation.value, cache_name, start, end, gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    if key_type:
        operation.setKeyType(key_type)

    return operation


def create_streamer_operation(cache_name, start, end,
                              with_exception=None,
                              value_type=None, parallel_operations=None, buff_size=None, allow_overwrite=None,
                              gateway=None):
    """
    Create streamer operation

    It puts values into defined cache name and use Long key.

    :param with_exception: throw exception o execution or just print stacktrace
    :param cache_name: cache name
    :param start: start key
    :param end: end key
    :param value_type: value type (default Long, optional - see org.apache.ignite.piclient.model entries)
    e.g. 'org.apache.ignite.piclient.model.values.AllTypes'
    :param parallel_operations: number of parallel operations
    :param buff_size: buffer size
    :param allow_overwrite: allowOverwrite flag
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.streamer_operation.value, cache_name, start, end, gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    if value_type:
        operation.setValueType(value_type)

    if parallel_operations:
        operation.setParallelOperations(parallel_operations)

    if buff_size:
        operation.setBuffSize(buff_size)

    if allow_overwrite:
        operation.setAllowOverwrite(allow_overwrite)

    return operation


def create_put_all_operation(cache_name, start, end, batch_size,
                             key_type=None,
                             value_type=None,
                             with_exception=None,
                             gateway=None):
    """
    Create streamer operation

    It puts values into defined cache name and use Long key.

    :param with_exception: throw exception o execution or just print stacktrace
    :param cache_name: cache name
    :param start: start key
    :param end: end key
    :param batch_size: batch size to put
    :param key_type: key type (default Long, optional - see org.apache.ignite.piclient.model entries)
    :param value_type: value type (default Long, optional - see org.apache.ignite.piclient.model entries)
    e.g. 'org.apache.ignite.piclient.model.values.AllTypes'
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.put_all_operation.value, cache_name, start, end, batch_size,
                                  gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    if value_type:
        operation.setValueType(value_type)

    if key_type:
        operation.setKeyType(key_type)

    return operation


def create_transactional_put_all_operation(
        values_to_cache, start, end, batch_size,
        tx_description=TxDescriptor(),
        with_exception=None,
        gateway=None):
    """
    Create tx put all operation

    It puts values into defined cache name and use Long key.

    :param with_exception: throw exception o execution or just print stacktrace
    :param values_to_cache: cache name
    :param start: start key
    :param end: end key
    :param batch_size: batch put size
    :param tx_description: TxDescriptor object
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.tx_put_operation.value,
                                  values_to_cache, start, end, batch_size, gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    _add_allowed_transactions_to_loading(operation, (tx_description,), gateway)

    return operation


def create_sum_operation(cache_name, start, end, field_name,
                         with_exception=None,
                         gateway=None):
    """
    Create streamer operation

    It puts values into defined cache name and use Long key.

    :param with_exception: throw exception o execution or just print stacktrace
    :param cache_name: cache name
    :param start: start key
    :param end: end key
    :param field_name: field to sum
    :param gateway: py4j gateway
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.sum_operation.value,
                                  cache_name, start, end, field_name, gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    return operation


def create_transaction_control_operation(
        cache_name, key, value,
        tx_description=TxDescriptor(),
        with_exception=None,
        gateway=None):
    """
    Create transaction operation and return control

    :param tx_description:
    :param with_exception: throw exception o execution or just print stacktrace
    :param cache_name: cache name
    :param key: key to put
    :param value: value to put
    :param tx_description: TxDescriptor object
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.tx_control_operation.value, cache_name, key, value, gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    _add_allowed_transactions_to_loading(operation, (tx_description,), gateway)

    return operation


def create_broke_data_entry_operation(cache_name, key, is_primary, *broke_types,
                                      with_exception=None,
                                      gateway=None):
    """
    Create broke partition operation (to touch idle_verify utility)

    Partition id will be automatically found for defined key (first will be taken) and returned as result
    Note that Long value should be used for cache

    :param with_exception: throw exception o execution or just print stacktrace
    :param cache_name: cache name
    :param key: key to broke
    :param is_primary: broke primary partition or backup
    :param broke_types: COUNTER/VALUE/INDEX or both
    :return: partitionId that was broken
    :param gateway: optional gateway (default taken from threading.current_thread)
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.broke_data_entry_operation.value,
                                  cache_name,
                                  key,
                                  is_primary,
                                  convert_to_java_array(gateway.jvm.java.lang.String, broke_types, gateway),
                                  gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    return operation


def create_change_entry_write_version_or_value_operation(cache_name, key, is_primary, *broke_types,
                                      with_exception=None,
                                      gateway=None):
    """
    Create broke data entry operation

    :param with_exception: throw exception o execution or just print stacktrace
    :param cache_name: cache name
    :param key: key to broke (only Long)
    :param is_primary: broke primary partition or backup
    :param broke_types: RANDOM_MINOR_WRITE_VERSION/CHANGE_VALUE or both
    :return: null
    :param gateway: optional gateway (default taken from threading.current_thread)
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.create_change_entry_write_version_or_value_operation.value,
                                  cache_name,
                                  key,
                                  is_primary,
                                  convert_to_java_array(gateway.jvm.java.lang.String, broke_types, gateway),
                                  gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    return operation


def clear_entry_locally_operation(cache_name, key, is_primary,
                                      with_exception=None,
                                      gateway=None):
    """
    Clear entry on node

    :param with_exception: throw exception o execution or just print stacktrace
    :param cache_name: cache name
    :param key: key to broke
    :param is_primary: broke primary partition or backup
    :return: null
    :param gateway: optional gateway (default taken from threading.current_thread)
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.clear_entry_locally_operation.value,
                                  cache_name,
                                  key,
                                  is_primary,
                                  gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    return operation


def create_account_runner_operation(cache_name, start, end, commit_possibility, allowed_transactions=None,
                                    delay=None, run_for_seconds=None,
                                    with_exception=None,
                                    gateway=None):
    """
    Operation that transfer money from one account into another
    Used cache should contains 'org.apache.ignite.piclient.model.values.Account' values

    :param with_exception: throw exception o execution or just print stacktrace
    :param cache_name: cache name
    :param start: start key
    :param end: end key
    :param run_for_seconds: how long proceed this type of operations (in seconds)
    :param commit_possibility: commit possibility (values in [0,1])
    :param allowed_transactions: list of TxDescriptor objects
    :param delay: delay between transactions (in milliseconds)
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.single_cache_transfer_operation.value, cache_name, start, end,
                                  commit_possibility,
                                  gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    if run_for_seconds:
        operation.setRunForSeconds(run_for_seconds)

    if delay:
        operation.setDelay(delay)

    if with_exception is not None:
        operation.setWithException(with_exception)

    _add_allowed_transactions_to_loading(operation, allowed_transactions, gateway)

    return operation


def create_cross_cache_account_runner_operation(cache_names, start, end, commit_possibility,
                                                allowed_transactions=None, delay=None, run_for_seconds=None,
                                                keys_count=None,
                                                collision_possibility=None,
                                                cache_load_map=None,
                                                with_exception=None,
                                                gateway=None):
    """
    Operation that transfer money from one account into another between caches
    Used caches should contains 'org.apache.ignite.piclient.model.values.Account' values

    :param with_exception: throw exception o execution or just print stacktrace
    :param cache_names: cache names
    :param start: start key
    :param end: end key
    :param run_for_seconds: how long proceed this type of operations (in seconds)
    :param cache_load_map: map that contains additional loading desriptors:
    cache name -> {
        'affinity_node_id': affinity node id (NOT NODE_ID in Ignite.app)
        'include': does include node to affinity node
        'metric_postfix': metrics postfix that will be added to plot
        (tx_metrics list that passed to TransactionalLoading should be also modified)
    }

    :param keys_count: keys count to affinity count key generator
    :param collision_possibility: possibility to chose key that was already used
    with this possibility last one of 10 used keys in loading will be selected
    :param commit_possibility: commit possibility (values in [0,1])
    :param allowed_transactions: list of TxDescriptor objects
    :param delay: delay between transactions (in milliseconds)
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.multicache_transfer_operation.value,
                                  convert_to_java_array(gateway.jvm.java.lang.String, cache_names, gateway),
                                  start,
                                  end,
                                  commit_possibility,
                                  gateway=gateway)

    if with_exception is not None:
        operation.setWithException(with_exception)

    if cache_load_map and keys_count:
        for cache in cache_names:
            if cache_load_map[cache]['key_generator_builder']:
                operation.setKeyGenerator(cache,
                                          cache_load_map[cache]['key_generator_builder'].set_gateway(gateway).build())

            if cache_load_map[cache]['metric_postfix']:
                operation.setMetricPostfix(cache_load_map[cache]['metric_postfix'])

    if run_for_seconds:
        operation.setRunForSeconds(run_for_seconds)

    if delay:
        operation.setDelay(delay)

    if with_exception is not None:
        operation.setWithException(with_exception)

    _add_allowed_transactions_to_loading(operation, allowed_transactions, gateway)

    return operation


def create_starvation_in_fork_join_pool(gateway=None):
    """
    Provide starvation in JVM pool
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """

    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.starvation_in_fork_join_pool.value,
                                  gateway=gateway)

    return operation


def create_throw_custom_error_operation(error_class='', node_id='', gateway=None):
    """
    Provide call throw new java.lang.error_class call in specific JVM belongs to node with node_id
    :param error_class: error class to throw
    :param node_id: node_id that use for found specific JVM.
    :param gateway: optional gateway (default taken from threading.current_thread)
    :return:
    """
    gateway = get_gateway(gateway)

    operation = _create_operation(Operation.throw_custom_error_operation.value, error_class, node_id,
                                  gateway=gateway)

    return operation


def convert_to_java_array(java_type, python_array, gateway):
    typed_java_array = gateway.new_array(java_type, len(python_array))

    for i, arg in enumerate(python_array):
        typed_java_array[i] = arg

    return typed_java_array


def _add_allowed_transactions_to_loading(operation,
                                         allowed_transactions,
                                         gateway):
    """
    Convert Python dictionary with tx_description objects to a java HashMap and
    pass it into addTransactionType() method to AbstractTransactionOperation objects

    :param operation: operation to pass allowed transaction
    :param allowed_transactions: list TxDescriptor objects
    :param gateway: gateway
    :return:
    """
    if not allowed_transactions:
        allowed_transactions = (
            TxDescriptor(),  # default pessimistic repeatable_read tx
            TxDescriptor(concurrency='OPTIMISTIC', isolation='SERIALIZABLE')
        )

    for allowed_transaction in allowed_transactions:
        hash_map = gateway.jvm.java.util.HashMap()

        hash_map['concurrency'] = '{}'.format(allowed_transaction.concurrency)
        hash_map['isolation'] = '{}'.format(allowed_transaction.isolation)
        hash_map['timeout'] = '{}'.format(allowed_transaction.timeout)
        hash_map['size'] = '{}'.format(allowed_transaction.size)

        if allowed_transaction.label:
            hash_map['label'] = '{}'.format(allowed_transaction.label)

        operation.addTransactionType(hash_map)


def _create_operation(class_name, *args, gateway):
    object_class = gateway.jvm.java.lang.Object
    args_array = gateway.new_array(object_class, len(args))

    for i, arg in enumerate(args):
        args_array[i] = arg

    return gateway.entry_point.getOperationsService().createOperation(class_name,
                                                                      args_array)
