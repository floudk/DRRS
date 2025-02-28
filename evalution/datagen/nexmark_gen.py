import os
import yaml
from utils import create_producer,sleep_until
from nexmark_utils import next_event, NexmarkConfig, get_tps, get_warmup_tps_and_duration
import multiprocessing
from datetime import datetime
import logging
import sys
import math

CONFIG_FILE = r'./nexmark.yaml'
assert os.path.exists(CONFIG_FILE)


with open(CONFIG_FILE, 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

gen_config = NexmarkConfig(config)


def run_nexmark_generator(event, tps_per_process, process_index, tps_warmup_per_process, tpw_warmup_duration):
    # print(f"Generator process {process_index} started with {tps_per_process} TPS")
    logging.info(f"Generator process {process_index} started with {tps_per_process} TPS")

    topic="nexmark"
    producer = create_producer(topic)
    logging.info("Nexmark initialized and waiting for trigger...")
    event.wait()
    # print("Nexmark started generating events at timestamp", datetime.now().timestamp())
    logging.info("Nexmark started generating events at timestamp %f", datetime.now().timestamp())

    event_index = 0
    generated = 0

    current_time = datetime.now().timestamp()
    current_warmup = tps_warmup_per_process
    step_increase = (tps_per_process - tps_warmup_per_process) // tpw_warmup_duration
    while datetime.now().timestamp() - current_time < tpw_warmup_duration:
        for i in range(current_warmup):
            next_event(gen_config, event_index, generated, producer)
            generated += 1
            event_index += 1
        # gradually increase the tps to tps_per_process
        current_warmup += step_increase
        sleep_until(current_time + 1)
    

    stat = []
    while True:
        counts = {}
        current_time = datetime.now().timestamp()
        
        for i in range(tps_per_process):
            topic = next_event(gen_config, event_index, generated, producer)
            generated += 1
            event_index += 1

            if topic not in counts:
                counts[topic] = 0
            counts[topic] += 1
        
        if generated >= gen_config.events_num:
            break

        sleep_until(current_time + 1)

        real_rate = {}
        for topic in counts:
            real_rate[topic] = counts[topic] / (datetime.now().timestamp() - current_time)
        stat.append(real_rate)
        
        if len(stat) % 200 == 0:
            # print(f"Current generator speed: {counts} in {datetime.now().timestamp() - current_time} seconds")
            logging.info(f"Cpeed: {counts} in {datetime.now().timestamp() - current_time} seconds in thread {process_index}")

        if len(stat) == 800:
            break

    producer.flush()
    total_avg = {}
    for i in range(len(stat)):
        for topic in stat[i]:
            if topic not in total_avg:
                total_avg[topic] = 0
            total_avg[topic] += stat[i][topic]
    for topic in total_avg:
        total_avg[topic] /= len(stat)
    # print(f"Generator process {process_index} finished at timestamp", datetime.now().timestamp(), "with average rate", total_avg)
    logging.info(f"Generator process {process_index} finished at timestamp %f with average rate %s", datetime.now().timestamp(), total_avg)


def run_nexmark(event: multiprocessing.Event):
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(message)s')
    tps = get_tps(os.getenv('TOPIC'), gen_config)
    warmup_tps, warmup_duration = get_warmup_tps_and_duration(os.getenv('TOPIC'), gen_config)

    process_num = math.ceil(tps / gen_config.each_tps)
    process_num = max(4, process_num)  # use at least 4 processes
    tps_per_process = tps // process_num
    remaining_tps = tps % process_num

    tps_warmup_per_process = warmup_tps // process_num

    processes = []
    for i in range(process_num - 1):
        p = multiprocessing.Process(target=run_nexmark_generator, args=(event, tps_per_process, i, tps_warmup_per_process, warmup_duration))
        p.start()
        processes.append(p)

    run_nexmark_generator(event, tps_per_process + remaining_tps, process_num - 1, tps_warmup_per_process, warmup_duration)

    for p in processes:
        p.join()

    


    





