import traceback
from collections import OrderedDict
from concurrent.futures.thread import ThreadPoolExecutor
from os import mkdir
from threading import Thread

from tiden import TidenException, log_print, sleep, datetime, print_red, print_green, path
from .helper.operation_utils import create_async_operation, create_account_runner_operation, \
    create_cross_cache_account_runner_operation, TxDescriptor
from .piclient import get_gateway, PiClient, get_gateways, PiClientException
from .utils import PiClientIgniteUtils


class TransactionalLoading:
    """
    Start loading inside block with PiClient context

    Approximate code:

        checksum_before = calculate_checksum()

        for (transactional_cache in ignite.caches().batch()):
            start new thread:
                while (!interrupted):
                    tx.start() {
                        transactional_cacheX[Y].remove balance
                        transactional_cacheA[B].add balance
                    }

        for (atomic_cache in ignite.caches()):
            start new thread:
                while (!interrupted):
                    tx.start() {
                        account1.remove balance
                        account2.add balance
                    }

        { ... } <- your test here

        checksum_after = calculate_checksum()

        if fail_on_exit:
            raise Exception()

    All this code evaluated inside AsyncOperations, so it's (cores/2) threads with (1000/delay) operations/second


    To tune loading LoadingProfile class exists.

    """

    def __init__(self, test_class,
                 loading_profile=None,
                 **kwargs):
        """
        Constructor:

        :param test_class: test class (TestIgnite e.g.)
        :param loading_profile: loading profile object - tunes loading (see LoadingProfile class)

        Debug kwargs:

        :param debug: do print some additional info (NO)
        :param fail_on_exception: do fail test on checksums fails and exceptions if code fails
        :param kill_transactions_on_exit: kill all transactions on cluster on end of loading (cu needed)
        :param sleep_before_checksum: sleep before calculate checksums in the end of loading
        :param wait_before_kill: wait for seconds for future with op.getResult() to complete (default 120)

        Optional kwargs:

        :param with_exception: with exception in operations
        :param skip_consistency_check: do skip calculating sum over field
        :param on_exit_action: function run on start of __exit__ method
        :param ante_checksum_action: function to tune self.sum_before list (has no affect if skip_consistency_check)
        :param post_checksum_action: function to tune self.sum_after list (has no affect if skip_consistency_check)

        :param skip_atomic:
        skip operations for atomic caches default = True (otherwise pls set skip_consistency_check to True)

        :param cross_cache: launch cross cache loading (default = True, cross operations does not affect ATOMIC caches)
        :param cross_cache_batch: batch size for tx caches cross cache (for big amount of caches in grid)
        :param affinity_node_id: only all keys from selected node will be used in loading
        :param keys_count: used with affinity_node_id - number of keys that passed to generator
        for plain key generator it is equals (end - start)
        in case of affinity key generator, value might be differ
        :param cache_load_map:

        map with cache load configuration. looks like this (see commentaries)

                        {
                            CACHE_NAME: {
                                'key_generator_builder': see pt.piclient.helper.generator_utils,

                                # this is metrics postfix (need to separate different caches in plot)
                                'metric_postfix': 'rebalance',  # metrics postfix for plot
                            },
                            CACHE_NAME_NOT_IN_REBALANCE: {
                                'key_generator': see pt.piclient.helper.generator_utils,

                                # this is metrics postfix (need to separate different caches in plot)
                                'metric_postfix': 'no_rebalance',  # metrics postfix for plot
                            }
                        }

        :param tx_load_threads:
        operation multiplier for each cache (if single client and 8 threads -> 8 threads per cache will be used)

        :param config_file: client config file
        :param jvm_options: additional JVM options

        :param tx_metrics: collection of metrics to collect
        :param collect_timeout: timeout on metrics collection at Java side, msec (default 1000)
        :param collect_timeout_metrics_thread: timeout on metrics collection at Python side, msec (default 1000)
        """
        if not get_gateway():
            raise TidenException("TransactionalLoading should be launched inside PiClient wrapper")

        # todo support app model with singlegridtestcase
        if 'ignite' in kwargs:
            self.ignite = kwargs.get('ignite')
        else:
            self.ignite = test_class.ignite

        self.loading_profile = loading_profile if loading_profile is not None else LoadingProfile()

        # debug options
        self.debug = kwargs.get('debug', False)
        self.fail_on_exception = kwargs.get('fail_on_exception', True)

        # loading need control utility to shut down all running transactions if this property defined
        self.kill_transactions_on_exit = kwargs.get('kill_transactions_on_exit', False)
        if 'cu' in kwargs:
            self.cu = kwargs.get('cu')
        else:
            self.cu = self.ignite.cu

        self.sleep_before_checksum = kwargs.get('sleep_before_checksum', 0)
        self.wait_before_kill = kwargs.get('wait_before_kill', 120)

        # node options
        self.config_file = kwargs.get('config_file', None)
        if self.config_file is None:
            # todo in app.model you DO need to pass client config manually
            self.config_file = test_class.get_client_config()
        self.jvm_options = kwargs.get('jvm_options')

        # additional actions options
        self.with_exception = kwargs.get('with_exception', True)
        self.skip_consistency_check = kwargs.get('skip_consistency_check', False)
        self.on_exit_action = kwargs.get('on_exit_action', None)
        self.ante_checksum_action = kwargs.get('ante_checksum_action', None)
        self.post_checksum_action = kwargs.get('post_checksum_action', None)

        # additional loading options
        self.caches_to_run = kwargs.get('caches_to_run', ())
        self.skip_atomic = kwargs.get('skip_atomic', True)
        self.cross_cache = kwargs.get('cross_cache', True)
        self.cross_cache_batch = kwargs.get('cross_cache_batch', 10)
        self.cache_load_map = kwargs.get('cache_load_map', None)
        self.affinity_node_id = kwargs.get('affinity_node_id', None)
        self.keys_count = kwargs.get('keys_count', None)
        self.tx_load_threads = kwargs.get('load_threads', None)

        # no guarantee that atomic caches have tx consistency
        if not self.skip_atomic and not self.skip_consistency_check:
            log_print('There is ATOMIC caches in loading and enabled consistency check. Sum of balance fields may vary',
                      color='yellow')

        if self.cross_cache_batch == 1:
            print_red("Cross caches batch is 1")

        self.tx_metrics = kwargs.get('tx_metrics', [])
        if len(self.tx_metrics) > 0:
            gateways = get_gateways()

            self.collect_timeout = kwargs.get('collect_timeout', 1000)

            for gateway in gateways:
                log_print('Will collect metric from gateway %s' % repr(gateway))
                gateway.entry_point.getMetricsCollector().startMetrics()
                gateway.entry_point.getMetricsCollector().setMetricsTimeout(self.collect_timeout)

            self.metrics = OrderedDict({
                'custom_events': [[], [], [], []],
            })

            for metric in self.tx_metrics:
                self.metrics[metric] = [[], []]

            self.collect_timeout_metrics_thread = kwargs.get('collect_timeout_metrics_thread', 1000)

            self.metrics_thread = MetricsThread(self, gateways)

    def __enter__(self):
        print_green("Preparing loading with profile: %s" % self.loading_profile)

        if self.skip_consistency_check:
            print_green("Skipping calculating sum over field")
        else:
            print_green("Calculating sum over field before loading")
            self.sum_before = PiClientIgniteUtils.calculate_sum_by_field(
                self.ignite,
                self.config_file,
                start_key=self.loading_profile.start_key,
                end_key=self.loading_profile.end_key,
                jvm_options=self.jvm_options,
                debug=self.debug
            )

            self.sum_before_total = 0
            for val in self.sum_before.values():
                self.sum_before_total += val

            if self.ante_checksum_action:
                print_green("Evaluating start additional actions")
                self.ante_checksum_action(self.sum_before)

        self.load_ops = self.start_loading()

        if self.tx_metrics:
            self.metrics_thread.start()

        return self

    def __exit__(self, etype, value, traceback):
        if self.on_exit_action:
            print_green("Evaluating on exit action")
            self.on_exit_action()

        if self.tx_metrics:
            print_green('Shutdown executor')
            self.metrics_thread.interrupted = True
            self.metrics_thread.join()

        print_green("Wait transactions to stop")
        self.wait_end_loading(self.load_ops)

        if self.kill_transactions_on_exit:
            print_green("Killing all transactions in cluster")
            self.cu.kill_transactions(force=True)

        if self.skip_consistency_check:
            print_green("Skipping calculating sum over field")
        else:
            print_green("Calculating sum over field after loading")

            sleep(self.sleep_before_checksum)

            sum_after = PiClientIgniteUtils.calculate_sum_by_field(
                self.ignite,
                self.config_file,
                start_key=self.loading_profile.start_key,
                end_key=self.loading_profile.end_key,
                jvm_options=self.jvm_options,
                debug=self.debug
            )

            if self.post_checksum_action:
                print_green("Evaluating post additional actions")
                self.post_checksum_action(sum_after)

            self.sum_after_total = 0
            for val in sum_after.values():
                self.sum_after_total += val

            if self.cross_cache:
                self.validate_checksums(self.sum_before_total, self.sum_after_total)
            else:
                for key, value in sum_after.items():
                    self.validate_checksums(key, self.sum_before[key])

        if not self.fail_on_exception and (etype and value and traceback):
            raise Exception(traceback)

    def validate_checksums(self, sum_before_total, sum_after_total):
        total_equals = str(sum_after_total) == str(sum_before_total)
        output = "Broken sum:\nexpected - %s\nactual - %s" % (sum_before_total, sum_after_total)

        if self.fail_on_exception:
            assert total_equals, output
        elif not total_equals:
            print_red(output)
        else:
            print_green("Checksums are equal")

    def start_loading(self):
        async_operations = []
        with PiClient(self.ignite, self.config_file, jvm_options=self.jvm_options) as piclient:
            print_green("Starting loading")

            tx_caches = []
            atomic_caches = []
            ignite = piclient.get_ignite()
            gateway = piclient.get_gateway()

            # run only defined caches or all caches in grid
            caches_to_run = self.caches_to_run if len(self.caches_to_run) > 0 else ignite.cacheNames().toArray()

            for cache_name in caches_to_run:
                # run cross cache transfer task only for transactional caches
                if ignite.getOrCreateCache(cache_name).getConfiguration(
                        gateway.jvm.org.apache.ignite.configuration.CacheConfiguration().getClass()
                ).getAtomicityMode().toString() == 'TRANSACTIONAL':
                    tx_caches.append(cache_name)
                else:
                    atomic_caches.append(cache_name)

            tx_caches_batches_size = 0
            tx_caches_size = len(tx_caches)
            atomic_caches_size = len(atomic_caches)

            tx_caches.sort()
            atomic_caches.sort()

            # for tx cache run cross_cache or single cache transfer task
            if self.cross_cache:
                # split tx caches into batches to avoid job with big amount of caches
                tx_caches_batches = \
                    [tx_caches[x:x + self.cross_cache_batch] for x in range(0, len(tx_caches), self.cross_cache_batch)]

                tx_caches_batches_size = len(tx_caches_batches)

                for tx_caches_batch in tx_caches_batches:
                    threads = 1 if not self.tx_load_threads else self.tx_load_threads

                    for _ in range(0, threads):
                        async_operation = create_async_operation(
                            create_cross_cache_account_runner_operation,
                            tx_caches_batch,
                            self.loading_profile.start_key,
                            self.loading_profile.end_key,
                            self.loading_profile.commit_possibility,
                            cache_load_map=self.cache_load_map,
                            keys_count=self.keys_count,
                            allowed_transactions=self.loading_profile.allowed_transactions,
                            delay=self.loading_profile.delay,
                            run_for_seconds=self.loading_profile.run_for_seconds,
                            with_exception=self.with_exception,
                        )

                        async_operations.append(async_operation)
                        async_operation.evaluate()
            else:
                for sorted_cache_name in tx_caches:
                    async_operation = create_async_operation(
                        create_account_runner_operation,
                        sorted_cache_name,
                        self.loading_profile.start_key,
                        self.loading_profile.end_key,
                        self.loading_profile.commit_possibility,
                        allowed_transactions=self.loading_profile.allowed_transactions,
                        delay=self.loading_profile.delay,
                        run_for_seconds=self.loading_profile.run_for_seconds,
                        with_exception=self.with_exception,
                    )

                    async_operations.append(async_operation)
                    async_operation.evaluate()

            # for atomic cache launch single cache task only
            if not self.skip_atomic:
                for atomic_cache in atomic_caches:
                    async_operation = create_async_operation(
                        create_account_runner_operation,
                        atomic_cache,
                        self.loading_profile.start_key,
                        self.loading_profile.end_key,
                        self.loading_profile.commit_possibility,
                        allowed_transactions=self.loading_profile.allowed_transactions,
                        delay=self.loading_profile.delay,
                        run_for_seconds=self.loading_profile.run_for_seconds,
                        with_exception=self.with_exception,
                    )

                    async_operations.append(async_operation)
                    async_operation.evaluate()

        ops_num = len(async_operations)
        log_print("Created %s operations: tx cache - %s (%s operations), atomic caches - %s; thread multiplier - %s.\n"
                  "Note that every tx cache operation will be multiplied by thread multiplier."
                  % (ops_num,
                     tx_caches_size,
                     tx_caches_batches_size,
                     atomic_caches_size,
                     1 if not self.tx_load_threads else self.tx_load_threads))

        assert ops_num != 0, 'There is no operations for loading started. Check that loadable caches exists.'

        return async_operations

    def wait_end_loading(self, operations):
        def wait_operations_result(ops):
            for op in ops:
                op.getResult()

        # interrupting operations
        if not self.loading_profile.run_for_seconds:
            for operation in operations:
                operation.getOperation().interrupt()

        if self.wait_before_kill:
            executor = ThreadPoolExecutor()
            fut = executor.submit(wait_operations_result, operations)

            log_print("Wait for operation interrupting future to complete", color='yellow')
            try:
                fut.result(self.wait_before_kill)
            except Exception:
                log_print("Failed to wait interrupting future to complete. Going to kill transactions.", color='red')
                traceback.print_exc()

                self.kill_transactions_on_exit = True

        if len(self.tx_metrics) > 0:
            for gateway in get_gateways():
                gateway.entry_point.getMetricsCollector().stopMetrics()


class MetricsThread(Thread):
    def __init__(self, loading, gateways):
        Thread.__init__(self)

        self.loading = loading
        self.gateways = gateways
        self.loading_timer = 0
        self.interrupted = False
        self.metrics = loading.metrics
        self.collect_timeout = loading.collect_timeout_metrics_thread

    def run(self):
        tick_time_sec = self.collect_timeout / 1000
        while not self.interrupted:
            sleep(tick_time_sec)
            self.loading_timer += tick_time_sec

            all_operations = {}
            for metric_name, metric_value in self.metrics.items():
                if metric_name != 'custom_events':
                    all_operations[metric_name] = 0.0

            # get calculated metrics over all launched piclients
            for gateway in self.gateways:
                for metric_name in self.metrics.keys():
                    if metric_name != 'custom_events':
                        metric_value = gateway.entry_point.getMetricsCollector().getPerSecond(metric_name)
                        if self.loading.debug and self.loading_timer % 100 == 0:
                            log_print('Got %s = %s from gw %s' % (metric_name, metric_value, repr(gateway)))

                        all_operations[metric_name] += metric_value

            for metric_name, metric_value in all_operations.items():
                self.loading.metrics[metric_name][1].append(metric_value)
                self.loading.metrics[metric_name][0].append(self.loading_timer)

    def add_custom_event(self, event_name='Unknown event'):
        self.loading.metrics['custom_events'][0].append(self.loading_timer)  # timestamp
        self.loading.metrics['custom_events'][1].append(0)  # y coord
        self.loading.metrics['custom_events'][2].append(event_name)  # event name
        self.loading.metrics['custom_events'][3].append(datetime.now())  # event time


class LoadingProfile:
    """
    Tunes Loading in TransactionalLoading

    All parameters passes to AccountRunnerOperation:
    key_start - start key
    key_end - end key
    run_for_seconds - evaluation time (note that there will be cores/2 threads and ExecutionPool with all caches)
    commit_possibility - 0<x<1 commit transaction possibility
    delay - sleep time between transactions
    allowed_transactions - list of TxDescriptor objects (default - ('PESSIMISTIC', 'REPEATABLE_READ', 0, 10000, None))
    """

    def __init__(self, **kwargs):
        self.start_key = kwargs.get('start_key', 1)
        self.end_key = kwargs.get('end_key', 1000)

        if self.start_key > self.end_key:
            raise PiClientException("start_key is greater than the end_key")

        self.commit_possibility = kwargs.get('commit_possibility', 0.5)

        self.delay = kwargs.get('delay', 1)
        self.run_for_seconds = kwargs.get('run_for_seconds', None)

        # obsolete value
        self.transaction_timeout = kwargs.get('transaction_timeout', 0)
        self.allowed_transactions = kwargs.get('allowed_transactions', (
            TxDescriptor(timeout=self.transaction_timeout),
            TxDescriptor(concurrency='OPTIMISTIC', isolation='SERIALIZABLE', timeout=self.transaction_timeout)
        ))

    def __str__(self):
        return "LoadingProfile(start=%s, " \
               "end=%s, " \
               "commit?=%s, " \
               "runForSeconds=%s, " \
               "delay=%s, " \
               "allowed_transactions=%s" % \
               (self.start_key,
                self.end_key,
                self.commit_possibility,
                self.run_for_seconds if self.run_for_seconds else "INFINITE",
                self.delay if self.delay else "1",
                self.allowed_transactions
                )


class LoadingUtils:
    @staticmethod
    def create_loading_metrics_graph(suite_var_dir, file_name, metrics, **kwargs):
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        attr_color = {
            "start": 'black',
            "end": 'black',
            " ": 'green'
        }
        plot_colors = ['b', 'g', 'r', 'c', 'm']

        fig, ax = plt.subplots()

        plt.style.use('ggplot')

        counter = 0
        for metric_name in metrics:
            metric_value = metrics[metric_name]

            if metric_name != 'custom_events':
                ax.plot(metric_value[0], metric_value[1], plot_colors[counter % 6], label=metric_name)
                counter += 1

        ax.legend(loc='upper left', shadow=True, fontsize=8)

        if 'custom_events' in metrics:
            custom_events_lines = []

            custom_events = metrics['custom_events']
            for id_event in range(0, len(custom_events[0])):
                custom_events_lines.append('%s. %s (%s)' % (str(int(id_event + 1)),
                                                            custom_events[2][id_event],
                                                            custom_events[3][id_event]))
                plt.annotate('%s' % str(int(id_event + 1)),
                             xy=(custom_events[0][id_event], custom_events[1][id_event]),
                             arrowprops=dict(facecolor='black', shrink=0.05), )

            plt.text(0, 50, '\n'.join(custom_events_lines), fontsize=8)

        plt.xlabel('time, s')
        plt.ylabel('transactions')
        plt.title('Transactions performance')
        plt.grid(True)

        if not path.exists('%s/test-plots' % suite_var_dir):
            mkdir('%s/test-plots' % suite_var_dir)

        fig = plt.gcf()
        dpi_factor = kwargs.get('dpi_factor', 1)
        fig.set_size_inches(22.5, 10.5)
        plt.savefig('%s/test-plots/plot-%s.png' % (suite_var_dir, file_name),
                    dpi=1000 * dpi_factor,
                    orientation='landscape')

    @staticmethod
    def sleep_and_custom_event(tx_loading, event_name='', timeout=20):
        print_green("Custom event added on plot: %s" % event_name)

        sleep(timeout)
        tx_loading.metrics_thread.add_custom_event(event_name)

    @staticmethod
    def custom_event(tx_loading, event_name=''):
        tx_loading.metrics_thread.add_custom_event(event_name)
