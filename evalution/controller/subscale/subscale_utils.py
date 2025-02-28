from config_gateway import *
from restful_gateway import *
from metric_collector import MetricCollector
import requests
from collections import Counter
import numpy as np
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename='/path/to/logs.log',
                    filemode='w')


def wait_for_scale(restful_gateway: RestfulGateway, with_state_init=False):
    restful_gateway.wait_for_scaling()
    restful_gateway.wait_for_cluster_update_completion()

def wait_for_complete(restful_gateway: RestfulGateway, metric_collector: MetricCollector):
    time.sleep(1)
    restful_gateway.complete_scale()
    restful_gateway.wait_for_canceling(metric_collector)

def split_keys_by_source_and_target(migrating_keys: dict):
    # for each (source, target) pair, store the keys
    source_target_migration = {}
    for key, subtask_indexes in migrating_keys.items():
        tup = tuple(subtask_indexes)
        if tup not in source_target_migration:
            source_target_migration[tup] = []
        source_target_migration[tup].append(key)
    return source_target_migration

def split_tupkeys_to_subscales(migrating_keys: dict, min_group_num=7)->dict:
    """
    return a dict, key is (source, target) tuple, value is a list of subscales
    """

    tup_keys = split_keys_by_source_and_target(migrating_keys)
    # calculate average number of keys for each tuple
    average_keys = sum([len(keys) for keys in tup_keys.values()]) / len(tup_keys)
    # use sqrt to calculate the number of keys involved in each subscale, if not integer, use ceil
    # fixed_subscale_key_num = ceil(sqrt(average_keys))
    fixed_subscale_key_num = max(min_group_num, int(average_keys//2))
    print("Fixed subscale key number: ", fixed_subscale_key_num, "(average: ", average_keys, ")")
    subscales_info = {}
    for source_target, keys in tup_keys.items():

        # print("left key num: ", left_key_num)
        need_extra = 1e10
        key_num = len(keys)
        length = fixed_subscale_key_num

        if key_num > fixed_subscale_key_num  and key_num % fixed_subscale_key_num != 0:
            group_num = key_num // fixed_subscale_key_num
            left_key_num = key_num % fixed_subscale_key_num
            
            length += (left_key_num // group_num)
            left_keys = left_key_num % group_num
            need_extra = group_num - left_keys
            # print("total keys: ", key_num, "group num: ", group_num, "modified subscale key num: ", fixed_subscale_key_num, "need extra: ", need_extra)

        group_count = 0
        while len(keys) > 0:
            if source_target not in subscales_info:
                subscales_info[source_target] = []
            # avoid 1 item in a subscale
            true_length = length + (1 if group_count >= need_extra else 0)
            subscales_info[source_target].append(keys[:true_length])
            keys = keys[true_length:]
            group_count +=1
    return subscales_info

class SystemStatus:
    def __init__(self, restful_gateway: RestfulGateway,key_to_tup_cache = None):
        self.restful_gateway = restful_gateway

        self.state_size = None
        self.new_parallelism = restful_gateway.config_gateway.new_parallelism

        # 0-not involved in any subscale
        # 1-involved in ongoing subscale: not transferred yet
        # 2-involved in ongoing subscale: already transferred but not (implicit) confirmed yet
        # 3-involved in finished subscale or finished in any ongoing subscale
        self.key_migration_status = [] # key(as index) -> migration status
        self.upstream_key_count = []
        self.upstream_key_rate = []
        self.processed_key_count = []
        self.key_locations = [] # key(as index) -> current location(as index)
        self.task_available = [] # task index -> available

        self.migrating_key_to_tup_cache = key_to_tup_cache
        self.task_hotness = []
        self.hotness = []

        if self.backpressure_enabled:
            restful_gateway.set_upstream_operators(drrs_specific_config.backpressure_metrics[restful_gateway.config_gateway.workload])
            self.backpressure = 0


    def get_state_size(self):
        state_size_map = self.restful_gateway.get_state_size_with_blocking()
        # covert map key->size to list(index as implicit key)
        state_size = []
        for i in range(len(state_size_map)):
            state_size.append(state_size_map[str(i)])
        self.state_size = np.array(state_size)
        # print("State size: ", self.state_size)
        logging.info("State size: "+str(self.state_size))

    def update(self, task_hotness_needed=False):

        metrics = self.restful_gateway.get_scale_metrics()
        # print("Metrics: ", metrics)
        # logging.info("Metrics: "+str(metrics))
        for metric in metrics:
            logging.info("Metrics: "+str(metric) + ": "+str(metrics[metric]))

        self.upstream_key_count = metrics.get("upstreamKeyCounts", None)
        self.upstream_key_rate = metrics.get("upstreamKeyRates", None)
        self.processed_key_count = metrics.get("processingKeyCounts", None)
        
        self.key_locations = metrics.get("stateLocation", None)
        self.task_available = metrics.get("taskAvailableStatus", None)
        self.key_migration_status = metrics.get("migrationStatus", None)

        if self.key_locations is None or self.task_available is None or self.key_migration_status is None or self.upstream_key_count is None or self.processed_key_count is None:
            print("Error: metrics not available")
            print("Metrics: ", metrics)
            raise Exception("Error: metrics not available")
        if task_hotness_needed:
            self.update_task_hotness()
            # print("Task hotness: ", self.task_hotness)
    
    def get_latency(self):
        return self.restful_gateway.get_latencies()

    def update_task_hotness(self):
        # reset self.task_hotness
        task_holding = []

        # hotness = upstream_key_count - processed_key_count
        self.hotness = np.array(self.upstream_key_count) - np.array(self.processed_key_count)

        self.task_hotness = []
        for i in range(self.new_parallelism):
            task_holding.append([])
            self.task_hotness.append(0)

        for i in range(len(self.key_locations)):
            loc = self.key_locations[i]
            if loc >= 0:
                task_holding[loc].append(i)
            else:
                assert loc == -1
                # the key temporarily not in any task: exactly in ongoing migration
                # we treat it as in the target task
                assert i in self.migrating_key_to_tup_cache
                task_holding[self.migrating_key_to_tup_cache[i][1]].append(i)
        
        for i in range(len(self.task_hotness)):
            for key in task_holding[i]:
                self.task_hotness[i] += self.hotness[key]


    def get_migrating_keys(self):

        migrating_keys = []
        for i in range(len(self.key_migration_status)):
            if self.key_migration_status[i] == 1 or self.key_migration_status[i] == 2:
                migrating_keys.append(i)
        return migrating_keys
    
    def get_available_tup(self, available_tup, conflict_concurrent):
        """
        key_status: list of key status
        return tuples that are allowed subsequent subscale under given conflict_concurrent
        """
        assert self.migrating_key_to_tup_cache is not None
        # remove tuples that own conflict subscales > conflict_concurrent
        conflict_count = Counter(
            self.migrating_key_to_tup_cache[key]
            for key in self.get_migrating_keys() 
            if self.migrating_key_to_tup_cache[key] in available_tup
        )
        available_tup -= {
            tup for tup, count in conflict_count.items() 
            if count > conflict_concurrent
        }

        # if any task is not available, remove the tuple
        # print(self.task_available)
        res = []
        for tup in available_tup:
            if self.task_available[tup[0]] and self.task_available[tup[1]]:
                res.append(tup)

        return res