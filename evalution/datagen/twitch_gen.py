import json
import time
import multiprocessing
import os
from utils import create_producer
from datetime import datetime
import logging


TWITCH_EVENTS=r'twitch_events.json'
assert os.path.exists(TWITCH_EVENTS)

def load_sorted_events_stream(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            event = json.loads(line.strip())  
            event["userId"] = int(event["userId"].split('.')[0])
            # map eventTime from [0, 420] -> [0, 1200]
            event["eventTime"] = event["eventTime"] * (1000 / 420)
            yield event 

def run_twitch_internal(event: multiprocessing.Event, file_list, gen_mode='natural', rate=2000):
    """
    natural: generate events in its natural time
    fixed: generate events in fixed rate
    """

    topic="twitch"
    producer = create_producer(topic)
    if gen_mode == 'fixed':
        logging.info("Twitch initialized and waiting for trigger with mode %s and rate %d", gen_mode, rate)
    else:
        logging.info("Twitch initialized and waiting for trigger with mode %s with file list %s", gen_mode, file_list)
    event.wait()
    print("Twitch started generating events at timestamp", datetime.now().timestamp())

    # responsible for all files in the file_list
    generators = []
    cached_events = []
    finished = [False] * len(file_list)
    start_time = time.time()

    for i, file_path in enumerate(file_list):
        gen = load_sorted_events_stream(file_path)
        generators.append(gen)
        try:
            event = next(gen)
            cached_events.append(event)
        except StopIteration:
            finished[i] = True 
            cached_events.append(None)
    
    if gen_mode == 'natural':
        # can not warm-up for natural mode, so we just sleep for 20s
        time.sleep(20)
        start_time = time.time()

        while True:
            current_time = time.time() - start_time

            for idx, event in enumerate(cached_events):
                if event is not None and event["eventTime"] <= current_time:
                    producer.produce(topic, json.dumps(event))
                    while True:
                        try:
                            new_event = next(generators[idx])
                            if new_event["eventTime"] <= current_time:
                                # change eventTime to current_time when producing
                                new_event["eventTime"] = int(time.time() * 1000)
                                producer.produce(topic, json.dumps(new_event))
                            else:
                                cached_events[idx] = new_event
                                break
                        except StopIteration:
                            finished[idx] = True
                            cached_events[idx] = None
                            break
            producer.flush()
            if all(finished) and all(event is None for event in cached_events):
                break
            min_next_time = min([event["eventTime"] for event in cached_events if event is not None])
            time_to_sleep = min_next_time - current_time
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)
    elif gen_mode == 'fixed':
        # add a 30s warm-up period: low rate: 1/10 of the rate
        warmup_interval = 30
        warmup_rate = int(rate / 10)
        while time.time() - start_time < warmup_interval:
            start_interval = time.perf_counter()
            count = 0
            while count < warmup_rate:
                for idx in range(len(cached_events)):
                    if not finished[idx]:
                        try:
                            event = next(generators[idx])
                            event["eventTime"] = int(time.time() * 1000)
                            producer.produce(topic, json.dumps(event))
                            count += 1
                        except StopIteration:
                            finished[idx] = True
                            cached_events[idx] = None
                        if count >= warmup_rate:
                            break
            producer.flush()
            time_to_sleep = 1 - (time.perf_counter() - start_interval)
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)

        batch_size = max(1, rate // 10)
        real_rate_stat = []
        while True:
            if all(finished) and all(event is None for event in cached_events):
                break
            start_interval = time.perf_counter()
            count_in_sec = 0

            for _ in range(10):  
                count = 0
                while count < batch_size:
                    for idx in range(len(cached_events)):
                        if not finished[idx]:
                            try:
                                event = next(generators[idx])
                                event["eventTime"] = int(time.time() * 1000)
                                producer.produce(topic, json.dumps(event))
                                count += 1
                                count_in_sec += 1
                            except StopIteration:
                                finished[idx] = True
                                cached_events[idx] = None
                        if count >= batch_size:
                            break
                producer.flush()

            elapsed = time.perf_counter() - start_interval # unit: second
            
            # print each 50 intervals: average rate

            time_to_sleep = 1 - elapsed
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)
            real_rate_stat.append(count_in_sec / (time.perf_counter() - start_interval))
            if len(real_rate_stat) % 50 == 0:
                # print("Recent avg gen rate:", sum(real_rate_stat) / len(real_rate_stat))
                logging.info("Recent avg gen rate: %f", sum(real_rate_stat) / len(real_rate_stat))

def run_twitch(event: multiprocessing.Event):
    folder = r'/app/events'
    file_list = os.listdir(folder)


    mode = 'natural'
    # mode = 'fixed'
    each_rate = None

    if mode == 'natural':
        print("Twitch will generate events in natural time")
        # total 100 files, but we do not process all of them, since we use containers to set up the environment
        used_process_count = 11
        each_process_file_count = 2
        # TODO: may need to change in fixed mode, since we do not need to output as events' time.
        file_groups = [file_list[i*each_process_file_count:(i+1)*each_process_file_count] for i in range(used_process_count)]
        # add the folder path to the file names
        file_groups = [[os.path.join(folder, file) for file in files] for files in file_groups]
    elif mode == 'fixed':
        total_rate = 180000
        used_process_count = 10
        each_process_file_count = 3
        file_groups = [file_list[i*each_process_file_count:(i+1)*each_process_file_count] for i in range(used_process_count)]
        file_groups = [[os.path.join(folder, file) for file in files] for files in file_groups]
        each_rate = total_rate / len(file_groups)
        each_rate = int(each_rate)

    logging.info("Twitch will use %d processes to generate events", len(file_groups))

    processes = []
    for i, files in enumerate(file_groups):
        p = multiprocessing.Process(target=run_twitch_internal, args=(event, files, mode, each_rate))
        p.start()
        processes.append(p)
    
    for p in processes:
        p.join()