
def compute_key_group_range_for_operator_index(max_parallelism: int, parallelism: int, operator_index: int):
    if max_parallelism < parallelism:
        raise ValueError("Maximum parallelism must not be smaller than parallelism.")

    start = (operator_index * max_parallelism + parallelism - 1) // parallelism
    end = ((operator_index + 1) * max_parallelism - 1) // parallelism

    return start, end


def compute_key_partitions(max_parallelism: int, parallelism: int):
    """
    return a list of tuples, each tuple is (start_key, end_key) for each subtask
    index of the list is the subtask index
    """
    if max_parallelism < parallelism:
        raise ValueError("Maximum parallelism must not be smaller than parallelism.")
    
    key_partitions = []
    for i in range(parallelism):
        start, end = compute_key_group_range_for_operator_index(max_parallelism, parallelism, i)
        key_partitions.append((start, end))
    
    return key_partitions
    

def calculate_migrating_keys(max_key, old_parallelism, new_parallelism)->dict:
    """
    Calculate the migrating keys for each subtask
    return a dict, key -> [old_subtask_index, new_subtask_index]
    """
    old_key_partitions = compute_key_partitions(max_key, old_parallelism)
    # print("Old key partitions: ", old_key_partitions)
    new_key_partitions = compute_key_partitions(max_key, new_parallelism)
    # print("New key partitions: ", new_key_partitions)
    key_with_both_partitions = []

    for subtask_index, partition in enumerate(old_key_partitions):
        for key in range(partition[0], partition[1] + 1):
            key_with_both_partitions.append([])
            key_with_both_partitions[-1].append(subtask_index)
    for subtask_index, partition in enumerate(new_key_partitions):
        for key in range(partition[0], partition[1] + 1):
            key_with_both_partitions[key].append(subtask_index)
    
    
    migrating_keys = {}
    for key, subtask_indexes in enumerate(key_with_both_partitions):
        if subtask_indexes[0] != subtask_indexes[1]:
            migrating_keys[key] = subtask_indexes

    return migrating_keys

    