from random import random

import json

from tiden import log_print, time, log_put, sleep, TidenException, print_warning, print_blue
from .helper.class_utils import ModelTypes
from .helper.operation_utils import create_async_operation, create_put_all_operation, \
    create_streamer_operation, create_transactional_put_all_operation, create_distributed_checksum_operation, \
    create_checksum_operation, create_sum_operation, TxDescriptor, create_transaction_control_operation, \
    create_remove_operation
from .piclient import PiClient, get_gateway


class PiClientIgniteUtils:
    """
    Utils static class

    Method used for control Ignite using launched piclient instance
    """

    @staticmethod
    def load_data_with_streamer_batch(
            ignite,
            config,
            cache_names=None,
            start_key=0, end_key=1001,
            buff_size=None,
            value_type=ModelTypes.VALUE_ACCOUNT.value,
            jvm_options=None,
            allow_overwrite=None,
            parallel_operations=1,
            check_clients=True,
            silent=False
    ):
        """
        Preloading based on piclient

        This method allows to load data with streamer by batches (e.g. fast stream data into single cache)

        :param ignite: app.ignite instance
        :param config: ignite configuration file name
        :param cache_names: cache names
        :param start_key: start key
        :param end_key: end key
        :param buff_size: buffer size
        :param value_type: e.g 'org.apache.ignite.piclient.model.values.Account'
        :param jvm_options: optional JVM options
        :param allow_overwrite:
        :param parallel_operations: parallel operations to stream into caches
        :param check_clients: do check topology snapshot
        :param silent: do logging
        :return:
        """
        if not silent:
            log_print("Load data into current caches using streamer")

        current_client_num = ignite.get_nodes_num('client')

        with PiClient(ignite, config, jvm_options=jvm_options) as piclient:
            piclients_num = len(piclient.node_ids)

            cache_names = piclient.get_ignite().cacheNames().toArray() if not cache_names else cache_names
            if not silent:
                log_print("Loading %s values per cache into %s caches" % (end_key - start_key, len(cache_names)))

            async_operations = []
            prev_end = start_key
            step = int(end_key / piclients_num)
            for i in range(1, piclients_num + 1):
                key_start_load = prev_end
                key_end_load = prev_end + step

                if i == piclients_num:
                    key_end_load = int(end_key)

                for cache_name in cache_names:
                    async_operation = create_async_operation(create_streamer_operation, cache_name,
                                                             key_start_load, key_end_load,
                                                             buff_size=buff_size,
                                                             value_type=value_type,
                                                             parallel_operations=parallel_operations,
                                                             allow_overwrite=allow_overwrite)
                    async_operations.append(async_operation)
                    async_operation.evaluate()

                prev_end = key_end_load

            for async_op in async_operations:
                async_op.getResult()

        if not silent:
            log_print("Preloading done")

        if check_clients:
            PiClientIgniteUtils.wait_for_running_clients_num(ignite, current_client_num, 120)

    @staticmethod
    def load_data_with_streamer(
            ignite,
            config,
            cache_names=None,
            start_key=0, end_key=1001,
            buff_size=None,
            value_type=ModelTypes.VALUE_ACCOUNT.value,
            jvm_options=None,
            allow_overwrite=None,
            nodes_num=None,
            check_clients=True,
    ):
        """
        Preloading based on piclient

        :param ignite: app.ignite instance
        :param config: ignite configuration file name
        :param cache_names: cache names
        :param start_key: start key
        :param end_key: end key
        :param buff_size: buffer size
        :param value_type: e.g 'org.apache.ignite.piclient.model.values.Account'
        :param jvm_options: optional JVM options
        :param allow_overwrite:
        :param nodes_num: nodes to start
        :param check_clients: do check topology snapshot
        :return:
        """
        log_print("Load data into current caches using streamer")

        current_client_num = ignite.get_nodes_num('client')

        with PiClient(ignite, config, jvm_options=jvm_options, nodes_num=nodes_num) as piclient:
            cache_names = piclient.get_ignite().cacheNames().toArray() if not cache_names else cache_names
            log_print("Loading %s values per cache into %s caches" % (end_key - start_key, len(cache_names)))

            async_operations = []
            for cache_name in cache_names:
                async_operation = create_async_operation(create_streamer_operation, cache_name, start_key, end_key,
                                                         buff_size=buff_size,
                                                         value_type=value_type,
                                                         parallel_operations=1,
                                                         allow_overwrite=allow_overwrite)
                async_operations.append(async_operation)
                async_operation.evaluate()

            for async_op in async_operations:
                async_op.getResult()

        log_print("Preloading done")

        if check_clients:
            PiClientIgniteUtils.wait_for_running_clients_num(ignite, current_client_num, 120)

    @staticmethod
    def load_random_data_with_streamer(
            ignite,
            config,
            start_key=0, end_key=1000,
            value_type=ModelTypes.VALUE_ACCOUNT.value,
            jvm_options=None,
            allow_overwrite=True,
            check_clients=True,
    ):
        """
        Preloading based on piclient

        :param ignite: app.ignite instance
        :param config: ignite configuration file name
        :param start_key: start key
        :param end_key: end key
        :param value_type: e.g 'org.apache.ignite.piclient.model.values.Account'
        :param jvm_options: optional JVM options
        :param allow_overwrite: allowOverwrite flag
        :param check_clients: do check topology snapshot
        :return:
        """
        from random import randint

        log_print("Load random data to caches from start to rand(start, end)")

        current_client_num = ignite.get_nodes_num('client')

        with PiClient(ignite, config, jvm_options=jvm_options) as piclient:
            cache_names = piclient.get_ignite().cacheNames()
            log_print("Loading %s values per cache into %s caches" % (end_key - start_key, cache_names.size()))

            async_operations = []
            for cache_name in cache_names.toArray():
                rand_start = randint(start_key, end_key)
                async_operation = create_async_operation(create_streamer_operation, cache_name, start_key,
                                                         rand_start,
                                                         value_type=value_type,
                                                         parallel_operations=1, allow_overwrite=allow_overwrite)
                async_operations.append(async_operation)
                async_operation.evaluate()

            for async_op in async_operations:
                async_op.getResult()

        log_print("Random loading done")

        if check_clients:
            PiClientIgniteUtils.wait_for_running_clients_num(ignite, current_client_num, 120)

    @staticmethod
    def load_data_with_putall(
            ignite,
            config,
            start_key=0,
            end_key=1001,
            batch_size=100,
            key_type='java.lang.Long',
            value_type=ModelTypes.VALUE_ACCOUNT.value,
            key_map=None,
            jvm_options=None,
            nodes_num=None,
            check_clients=True,
            wait_for_indexes=False,
            with_exception=None,
    ):
        """
        Preloading based on piclient

        :param ignite: app.ignite instance
        :param config: ignite configuration file name
        :param start_key: start key
        :param end_key: end key
        :param key_type: e.g 'org.apache.ignite.piclient.model.values.Account'
        :param value_type: e.g 'org.apache.ignite.piclient.model.values.Account'
        :param jvm_options: optional JVM options
        :param nodes_num: nodes to start
        :param check_clients: do check topology snapshot
        :return:
        """
        log_print("Load current caches using putAll operation")
        current_client_num = len(ignite.get_alive_common_nodes())

        with PiClient(ignite, config, jvm_options=jvm_options, nodes_num=nodes_num) as piclient:
            cache_names = piclient.get_ignite().cacheNames()
            log_print("Loading %s values per cache into %s caches" % (end_key - start_key, cache_names.size()))

            async_operations = []
            for cache_name in cache_names.toArray():
                async_operation = create_async_operation(create_put_all_operation,
                                                         cache_name,
                                                         start_key,
                                                         end_key,
                                                         batch_size,
                                                         key_type=key_map[cache_name] if key_map else key_type,
                                                         value_type=value_type,
                                                         with_exception=with_exception)
                async_operations.append(async_operation)
                async_operation.evaluate()

            for async_op in async_operations:
                async_op.getResult()

            if wait_for_indexes:
                for cache_name in cache_names.toArray():
                    piclient.get_ignite().cache(cache_name).indexReadyFuture().get()

        log_print("Preloading done")

        if check_clients:
            PiClientIgniteUtils.wait_for_running_clients_num(ignite, current_client_num, 120)

    @staticmethod
    def load_data_with_txput_sbt_model(
            tiden_config,
            ignite,
            config,
            start_key=0, end_key=99, jvm_options=None,
            only_caches_batch=None,
            to_job_cache_batch=20,
            tx_put_batch=500,
            random_tx_timeout=10,
    ):
        """
        Preloading SBT model with transactional put

        Recommended invariant:

        (end_key - start_key) - number of inserted items per cache
        to_job_cache_batch - number of caches that passed into single put operation
        tx_put_batch - number of entries that will be inserted inside one transaction

        1. (end_key - start_key) * to_job_cache_batch >> tx_put_batch
        2. (end_key - start_key) << tx_put_batch

        First invariant guarantee that in each job there will be few transactions with cross cache put
        Second invariant guarantee that

        :param tiden_config: tiden configuration (to get resource directory)
        :param ignite: app.ignite instance
        :param config: ignite configuration file name
        :param start_key: start key
        :param end_key: end key
        :param jvm_options: optional JVM options
        :param only_caches_batch: put in first n caches (None - all caches in descriptor will be used)
        :param to_job_cache_batch: caches in batch to put (e.g. 3 caches in batch will be inserted in 1 tx)
        :param tx_put_batch: put values batch (should be cross cached)
        :param random_tx_timeout:0 then normal loading else with 50% possibility random_tx_timeout value will be used
        :return:
        """
        if (end_key - start_key) * to_job_cache_batch < tx_put_batch:
            print_warning("Check load_data_with_txput_sbt_model() arguments. "
                          "Recommended (end_key - start_key) * to_job_cache_batch >> tx_put_batch "
                          "for optimal performance and jobs distribution")

        if (end_key - start_key) > tx_put_batch:
            print_warning("Check load_data_with_txput_sbt_model() arguments. "
                          "Recommended (end_key - start_key) << tx_put_batch "
                          "for cross cache transactional insert")

        log_print("Load data using tx put into sbt model caches")
        current_client_num = ignite.get_nodes_num('client')

        with open("%s/json_model.json" % tiden_config['rt']['test_resource_dir'], 'r') as f:
            model_descriptor_file = f.read()

        model_descriptor = json.loads(json.loads(model_descriptor_file))

        # define transactions that should be used in test (random with small timeout and normal)
        tx_description_random_timeout = TxDescriptor(timeout=random_tx_timeout)
        tx_description_default = TxDescriptor()

        # launch piclient and load data using SBT profile
        with PiClient(ignite, config, jvm_options=jvm_options) as piclient:
            gateway = get_gateway()
            values_to_cache = gateway.jvm.java.util.HashMap()

            async_operations = []

            descriptor_size = len(model_descriptor)
            counter = 0

            if only_caches_batch and isinstance(only_caches_batch, int):
                # get list of (obj_name, cache_name) multiplied by some value to get a lot of put into only_caches_batch
                model_descriptor_items = list(model_descriptor.items())[0:only_caches_batch] * \
                                         int(descriptor_size / only_caches_batch)
            else:
                model_descriptor_items = model_descriptor.items()

            for obj_name, cache_name in model_descriptor_items:
                values_to_cache.put(obj_name, cache_name)

                counter += 1

                if len(values_to_cache) == to_job_cache_batch and \
                        (len(values_to_cache) == only_caches_batch if only_caches_batch else True) or \
                        counter == descriptor_size:
                    async_op = create_async_operation(create_transactional_put_all_operation,
                                                      values_to_cache, start_key, end_key, tx_put_batch,
                                                      tx_description=tx_description_random_timeout
                                                      if (random_tx_timeout != 0 and random() < .5)
                                                      else tx_description_default,
                                                      with_exception=False,
                                                      gateway=gateway)
                    async_operations.append(async_op)
                    async_op.evaluate()

                    gateway = piclient.get_gateway()
                    values_to_cache = gateway.jvm.java.util.HashMap()

            for async_op in async_operations:
                async_op.getResult()

        log_print("Load done")
        PiClientIgniteUtils.wait_for_running_clients_num(ignite, current_client_num, 120)

    @staticmethod
    def remove_data(
            ignite,
            config,
            cache_names=None,
            start_key=0, end_key=1001,
            key_type=None,
            jvm_options=None,
            nodes_num=None,
            check_clients=True,
    ):
        """
        Preloading based on piclient

        :param ignite: app.ignite instance
        :param config: ignite configuration file name
        :param cache_names: cache names
        :param start_key: start key
        :param end_key: end key
        :param key_type: e.g 'org.apache.ignite.piclient.model.Account'
        :param jvm_options: optional JVM options
        :param nodes_num: nodes to start
        :param check_clients: do check topology snapshot
        :return:
        """
        log_print("Removing data from current caches using cache.remove()")

        current_client_num = ignite.get_nodes_num('client')

        with PiClient(ignite, config, jvm_options=jvm_options, nodes_num=nodes_num) as piclient:
            cache_names = piclient.get_ignite().cacheNames().toArray() if not cache_names else cache_names
            log_print("Removing %s values per cache into %s caches" % (end_key - start_key, len(cache_names)))

            async_operations = []
            for cache_name in cache_names:
                async_operation = create_async_operation(create_remove_operation, cache_name, start_key, end_key,
                                                         key_type=key_type)
                async_operations.append(async_operation)
                async_operation.evaluate()

            for async_op in async_operations:
                async_op.getResult()

        log_print("Removing keys done")

        if check_clients:
            PiClientIgniteUtils.wait_for_running_clients_num(ignite, current_client_num, 120)

    @staticmethod
    def calc_checksums_distributed(
            ignite,
            config,
            jvm_options=None,
            check_clients=True,
            new_instance=False
    ):
        """
        Calculate checksum based on piclient

        :param ignite: app.ignite instance
        :param config: ignite configuration file name
        :param jvm_options: jvm options
        :param check_clients: do check topology snapshot
        :return:
        """
        log_print("Calculating checksums using distributed job")
        current_client_num = ignite.get_nodes_num('client')

        with PiClient(ignite, config, jvm_options=jvm_options, new_instance=new_instance):
            checksums = create_distributed_checksum_operation().evaluate()

        log_print('Calculating checksums done')

        if check_clients:
            PiClientIgniteUtils.wait_for_running_clients_num(ignite, current_client_num, 120)

        return checksums

    @staticmethod
    def calc_checksums_on_client(
            ignite,
            config,
            start_key=0,
            end_key=1000,
            dict_mode=False,
            jvm_options=None
    ):
        """
        Calculate checksum based on piclient

        :param ignite: app.ignite instance
        :param config: ignite configuration file name
        :param start_key: start key
        :param end_key: end key
        :param dict_mode:
        :param jvm_options: jvm options
        :return:
        """
        log_print("Calculating checksums using cache.get() from client")
        cache_operation = {}
        cache_checksum = {}

        current_client_num = ignite.get_nodes_num('client')

        with PiClient(ignite, config, jvm_options=jvm_options) as piclient:
            sorted_cache_names = []
            for cache_name in piclient.get_ignite().cacheNames().toArray():
                sorted_cache_names.append(cache_name)

            sorted_cache_names.sort()

            async_operations = []
            for cache_name in sorted_cache_names:
                async_operation = create_async_operation(create_checksum_operation, cache_name, start_key, end_key)
                async_operations.append(async_operation)
                cache_operation[async_operation] = cache_name
                async_operation.evaluate()

            checksums = ''

            for async_operation in async_operations:
                result = str(async_operation.getResult())
                cache_checksum[cache_operation.get(async_operation)] = result
                checksums += result

        log_print('Calculating checksums done')

        PiClientIgniteUtils.wait_for_running_clients_num(ignite, current_client_num, 120)

        if dict_mode:
            return cache_checksum
        else:
            return checksums

    @staticmethod
    def calculate_sum_by_field(
            ignite,
            config,
            start_key=0,
            end_key=1001,
            field='balance',
            debug=False,
            jvm_options=None
    ):
        """
        Calculate checksum based on piclient

        :param ignite: app.ignite instance
        :param config: ignite configuration file name
        :param start_key: start key
        :param end_key: end key
        :param field: class field name
        :param debug: print output
        :param jvm_options: jvm options
        :return:
        """
        log_print("Calculating sum over field '%s' in each cache value" % field)

        current_client_num = ignite.get_nodes_num('client')

        result = {}

        with PiClient(ignite, config, jvm_options=jvm_options) as piclient:
            sorted_cache_names = []
            for cache_name in piclient.get_ignite().cacheNames().toArray():
                sorted_cache_names.append(cache_name)

            sorted_cache_names.sort()

            if debug:
                print(sorted_cache_names)

            total = 0
            for cache_name in sorted_cache_names:
                operation_result = create_sum_operation(cache_name, start_key, end_key, field).evaluate()

                total += operation_result
                result[cache_name] = operation_result

                if debug:
                    print(cache_name)
                    print_blue('Bal %s' % operation_result)

            log_print('Calculating over field done. Total sum: %s' % total)

        PiClientIgniteUtils.wait_for_running_clients_num(ignite, current_client_num, 120)

        return result

    @staticmethod
    def wait_for_running_clients_num(ignite, client_num, timeout):
        """
        Waiting for given number of running client nodes
        :param ignite:  app.ignite instance
        :param client_num:  number of client nodes
        :param timeout:     timeout in seconds
        :return:
        """
        started = int(time())
        timeout_counter = 0
        current_num = 0
        while timeout_counter < timeout:
            current_num = ignite.get_nodes_num('client')
            if current_num == client_num:
                break
            log_put("Waiting running clients: %s/%s, timeout %s/%s sec"
                    % (current_num, client_num, timeout_counter, timeout))
            sleep(5)
            timeout_counter = int(time()) - started
        if current_num != client_num:
            raise TidenException('Waiting for client number failed')
        else:
            log_print("Found %s running client node(s) in %s/%s sec"
                      % (current_num, timeout_counter, timeout))

    @staticmethod
    def collect_cache_group_names(ignite, client_config):
        """
        Collect cache group names using piclient

        :param ignite: ignite app instance
        :param client_config: ignite configuration file name
        :return:
        """
        group_names = []
        with PiClient(ignite, client_config) as piclient:

            ignite = piclient.get_ignite()
            gateway = piclient.get_gateway()
            cacheNames = ignite.cacheNames().toArray()

            for cacheName in cacheNames:
                group_name = ignite.getOrCreateCache(cacheName).getConfiguration(
                    gateway.jvm.org.apache.ignite.configuration.CacheConfiguration().getClass()).getGroupName()
                if group_name:
                    group_names.append(group_name)
                else:
                    group_names.append(cacheName)

        return group_names

    @staticmethod
    def release_transactions(lrt_operations):
        for lrt_operation in lrt_operations.values():
            lrt_operation.releaseTransaction()

    @staticmethod
    def launch_transaction_operations(cache_name, start_key=0, end_key=10):
        lrt_operations = {}
        for i in range(start_key, end_key):
            tx_d = TxDescriptor(label='tx_%s' % i)
            lrt_operation = create_transaction_control_operation(cache_name, i, i,
                                                                 tx_description=tx_d)
            lrt_operation.evaluate()

            lrt_operations[i] = lrt_operation

        return lrt_operations
