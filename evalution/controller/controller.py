"""
The coordinator module is responsible for triggering subscales, including
1. Triggering scale processes
2. Triggering concrete subscales with delicate scheduling
3. Finishing scale processes
"""
import os
import sys
from restful_gateway import RestfulGateway
from config_gateway import ConfigGateway
from metric_collector import MetricCollector

from common_utils import *
from evaluator_drrs import evaluate_drrs
from evaluator_meces_and_megaphone import evaluate_others


def get_arg(args, index, default, availables):
    if len(args) > index and args[index] in availables:    
        return args[index]
    return default


def main():
    # if no argument is provided, use default values
    if len(sys.argv) == 1:
        print("No argument is provided, exiting...")
        exit(1)
    
    #print("running evaluation with args: ", sys.argv)
    mechanism = get_arg(sys.argv, 1, "drrs", ["unbound", "megaphone", "drrs", "meces", "otfs"])
    workload = get_arg(sys.argv, 2, "fwc", ["fwc", "q7", "q8", "twitch"])

    config_gateway = ConfigGateway(mechanism, workload)
    restful_gateway = RestfulGateway(config_gateway)
    metric_collector = MetricCollector(restful_gateway)

    restful_gateway.wait_for_master_available()
    ################## jar preparation and cluster initialization ##################
    restful_gateway.upload_jar_and_wait_for_cluster_available()
    restful_gateway.submit_job_and_wait_for_running()
    metric_collector.start_collecting()
    
    ################## run evaluation ##################
    if not workload == "fwc":
        # notify kafka producer to start generating events
        restful_gateway.notify_kafka_producer_start()

    mechanism2call = {
        "drrs": evaluate_drrs,
        "meces": evaluate_others,
        "megaphone": evaluate_others,
        "unbound": evaluate_others,
        "otfs": evaluate_others
    }
    evaluate = mechanism2call[mechanism]
    evaluate(restful_gateway, metric_collector)

    metric_output_path = config_gateway.metric_output_path

    statistics_log_path = os.path.join(metric_output_path, f"{mechanism}-{workload}-statistics.log")

    if workload == "fwc":
        state_size = config_gateway.load_fwc_state_size()
        input_rate = config_gateway.load_fwc_input_rate()
        skewness = config_gateway.load_fwc_skewness()
        restful_gateway.restful_statistics["state_size"] = state_size
        restful_gateway.restful_statistics["input_rate"] = input_rate
        restful_gateway.restful_statistics["skewness"] = skewness

    with open(statistics_log_path, "w") as f:
        for key, value in restful_gateway.restful_statistics.items():
            f.write(f"{key}: {value}\n")

if __name__ == "__main__":
    main()

