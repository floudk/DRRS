from subscale.subscale_utils import *
from restful_gateway import RestfulGateway
from metric_collector import MetricCollector
from common_utils import calculate_migrating_keys,compute_key_partitions

class MinHoldScheduler:
    """
    Let new subtasks get involved in the normal process as soon as possible
    """
    def __init__(self, 
                 restful_gateway: RestfulGateway, 
                 metric_collector: MetricCollector,
                 subscale_min_group_num=7,
                 conflict_concurrent=2):
        self.restful_gateway = restful_gateway
        self.metric_collector = metric_collector

        self.conflict_concurrent = conflict_concurrent

        new_parallelism = restful_gateway.config_gateway.new_parallelism
        old_parallelism = restful_gateway.config_gateway.old_parallelism
        max_key = restful_gateway.config_gateway.max_key

        self.migrating_keys = calculate_migrating_keys(max_key, old_parallelism, new_parallelism)
        self.emitted_subscale = []

        self.subscales_by_sttup = split_tupkeys_to_subscales(self.migrating_keys, min_group_num=subscale_min_group_num)
        
        total_migration_keys = 0
        for keys_info in self.subscales_by_sttup.values():
            for keys in keys_info:
                total_migration_keys += len(keys)
        print("Subscales info: ", self.subscales_by_sttup, total_migration_keys, "/", max_key, "keys need to be migrated")

        self.pending_subscales = 0
        for keys_info in self.subscales_by_sttup.values():
            # print(keys_info, len(keys_info))
            self.pending_subscales += len(keys_info)
        # print("Pending subscales: ", self.pending_subscales)

        self.now_processing_key_count = []
        now_partitions = compute_key_partitions(max_key, old_parallelism)
        for subtask_index, partition in enumerate(now_partitions):
            count = partition[1] - partition[0] + 1
            self.now_processing_key_count.append(count)
        # add new index
        for i in range(old_parallelism, new_parallelism):
            self.now_processing_key_count.append(0)
        print("Now processing key count: ", self.now_processing_key_count)
        self.ablation_study = restful_gateway.config_gateway.load_dull_scheduler_enabled()

        self.sys_tracker = SystemStatus(restful_gateway)

    def is_finished(self):
        return self.pending_subscales == 0
    
    def select_tup_with_max_subscales(self, subscale_index, avaliable_subtasks)->tuple:
        max_subscales = 0
        res_tup = None
        # print("selecting tup with max subscales for subtask: ", subscale_index, "with available subtasks: ", avaliable_subtasks)
        for tup, subscales in self.subscales_by_sttup.items():
            if tup[0] != subscale_index and tup[1] != subscale_index:
                # this subscale does not involve the subtask
                continue
            elif tup[0] in avaliable_subtasks or tup[1] in avaliable_subtasks:
                # print("tup: ", tup, "subscales num: ", len(subscales))
                if len(subscales) > max_subscales:
                    max_subscales = len(subscales)
                    res_tup = tup
            else:
                # one of the subtasks is not available
                continue
        return res_tup
    
    def count_involved_subscales_for_subtask(self, subtask_index):
        count = 0
        for tup in self.subscales_by_sttup.keys():
            if subtask_index in tup:
                count += len(self.subscales_by_sttup[tup])
        return count

    def get_next_subscale(self)-> list:
        """
        return serveral subscales: each is a list of keys,
        these subscales can be processed concurrently based on greedy algorithm to avoid resource contention
        """
        self.sys_tracker.update()
        ongoing_keys = self.sys_tracker.get_migrating_keys()


        if self.ablation_study:
            # return all subscales as a flat list
            all_subscales = []
            for subscales in self.subscales_by_sttup.values():
                for subscale in subscales:
                    all_subscales.extend(subscale)
            
            self.pending_subscales = 0

            return all_subscales

        assert self.pending_subscales > 0
        avaliable_subtasks = {i for i in range(self.restful_gateway.config_gateway.new_parallelism)}

        parallel_counts = {}
        ongoing_subscales = set()
        for ongoing_key in ongoing_keys:
            for subscale, tup in self.emitted_subscale:
                if ongoing_key in subscale:
                    ongoing_subscales.add((tuple(subscale), tup))
        
        for subscale, tup in ongoing_subscales:
            old_subtask_index, new_subtask_index = tup
            
            parallel_counts[new_subtask_index] = parallel_counts.get(new_subtask_index, 0) + 1
            parallel_counts[old_subtask_index] = parallel_counts.get(old_subtask_index, 0) + 1
            
            if parallel_counts[new_subtask_index] >= self.conflict_concurrent:
                avaliable_subtasks.discard(new_subtask_index)
            if parallel_counts[old_subtask_index] >= self.conflict_concurrent:
                avaliable_subtasks.discard(old_subtask_index)
        
        if len(avaliable_subtasks) <= 1:
            return []
        
        # sort avaliable subtasks by the number of processing keys
        sorted_avl_subtasks = {}
        for subtask in avaliable_subtasks:
            sorted_avl_subtasks[subtask] = self.now_processing_key_count[subtask]
        
        sorted_avl_subtasks = sorted(sorted_avl_subtasks.items(), key=lambda x: x[1])

        first_subscale = None
        while len(avaliable_subtasks) > 1:
            subtask = sorted_avl_subtasks[0][0]
            sorted_avl_subtasks.pop(0)
            if subtask not in avaliable_subtasks:
                continue
            avaliable_subtasks.discard(subtask)

            tup = self.select_tup_with_max_subscales(subtask, avaliable_subtasks)
            if tup is None:
                # no more subscales involving this subtask
                continue
            # print("selected tup: ", tup)
            # get the first subscale
            first_subscale = self.subscales_by_sttup[tup].pop(0)
            self.emitted_subscale.append((first_subscale, tup))
            self.pending_subscales -= 1

            # update the processing key count
            old_subtask_index, new_subtask_index = tup
            self.now_processing_key_count[old_subtask_index] -= len(first_subscale)
            self.now_processing_key_count[new_subtask_index] += len(first_subscale)
            break
        # print("Pending subscales: ", self.pending_subscales)

        return first_subscale
    
    def run(self):
        wait_for_scale(self.restful_gateway, with_state_init=False) # must need state collection
        start_time = time.time()
        skip_count = 0
        while not self.is_finished():
            start_time = time.time()
            subscale = self.get_next_subscale()
            if subscale:
                print("Trigger subscales:", subscale)
                self.restful_gateway.trigger_subscale(subscale)
                skip_count = 0
            else:
                # print("Skip this round...")
                # check if there a timeout (exceed 30min)
                skip_count += 1
            # keep 1s for a round
            left_time = 1 - (time.time() - start_time)
            if left_time > 0:
                time.sleep(left_time)
            else:
                print("Warning: exceed 1s for a round, using actual time: ", time.time() - start_time)
            
            if self.restful_gateway.config_gateway.workload == "fwc":
                if skip_count > 10 and self.restful_gateway.is_reach_canceling_point():
                    # the system may be stucked, need to check whether the system should be terminated
                    self.restful_gateway.restful_statistics["error"] = "System stucked, canceling the job"
                    break
        
        wait_for_complete(self.restful_gateway, self.metric_collector)
