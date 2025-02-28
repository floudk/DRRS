from restful_gateway import RestfulGateway
import time
import os
import threading

class MetricCollector():
    """
    Only collect the input rate in source operators,
    while FWC does not need to collect this metric.
    """
    def __init__(self, restful_gateway: RestfulGateway, collect_interval=1):
        self.restful_gateway = restful_gateway

        self.metric_output_path = self.restful_gateway.config_gateway.metric_output_path
        self.stop = False
        self.collect_interval = collect_interval

        self.threading = threading.Thread(target=self.run)

    def start_collecting(self):
        if not self.restful_gateway.config_gateway.workload == "fwc":
            self.threading.start()
        else:
            print("FWC does not need to collect metrics...")
            self.restful_gateway.init_vertex_id_only()
    
    def stop_collecting(self):
        self.stop = True
        
    def run(self):
        input_metrics = self.restful_gateway.get_input_metrics()
        print("Collecting metrics for", input_metrics)
        inputs = []

        while not self.stop:
            next_time = time.time() + self.collect_interval
            now = self.restful_gateway.get_now()
            for parallelism, vertex_id, metrics in input_metrics:
                replaced_metrics = []
                for i in range(parallelism):
                    for metric in metrics:
                        replaced_metrics.append(f"{i}.{metric}")
                    inputs.append((now, i, self.restful_gateway.get_vertex_metrics(vertex_id, replaced_metrics)))
            time.sleep(max(0, next_time - time.time()))

        output_file = os.path.join(self.metric_output_path, f"throughputs.log")
        with open(output_file, "w") as f:
            for timestamp, source_index, metrics in inputs:
                f.write(f"{timestamp}: {source_index}: {metrics}\n")
        print("Metrics collected and saved to file...")