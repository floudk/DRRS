import json
import os
import numpy as np

class ConfigGateway:
    def __init__(self, mechanism, workload):
        config_file = os.path.join(os.path.dirname(__file__), 'conf/config.json')
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        
        self.mechanism = mechanism
        self.workload = workload

        #################### master url ####################
        self.master_url = self.read_or_throw("master-url")
        # print(f"Master url: {self.master_url}")

        #################### jar path ####################
        mechanism_forlder = self.read_or_throw_nested(["mechanism-folder", mechanism])
        mechanism_path = "/path/to/project/Flink/" + mechanism_forlder + "/build-target/examples/streaming/"
        case_jar = self.read_or_throw_nested(["jar-path", workload])
        self.jar_path = os.path.join(mechanism_path, case_jar)
        if not os.path.exists(self.jar_path):
            raise FileNotFoundError(f"Jar path {self.jar_path} does not exist")
        # print(f"Jar path: {self.jar_path}")

        #################### taskmanager number ####################
        self.taskmanager_number = self.read_or_throw_nested(["taskmanagers", workload])

        #################### other config ####################
        self.max_key = self.read_or_throw_nested(["program-args", workload, "maxKeyNum"])

        #################### scaling ####################
        self.scaling_vertex_name = self.read_or_throw_nested(["scaling-vertex", workload])
        self.old_parallelism = self.read_or_throw_nested(["initial-parallelism", workload])
        self.new_parallelism = self.read_or_throw_nested(["scale-out-parallelism", workload])
        self.time_before_scale = self.read_or_throw_nested(["time-before-scale", workload])

        self.time_after_scale = self.read_or_throw_nested(["time-after-scale", workload])

        self.metric_output_path = self.read_or_throw("metric-output-path") + '/tmp'

        self.do_not_scale = self.read_or_throw("do-not-scale")


    def read_or_throw(self, key):
        if key not in self.config:
            raise ValueError(f"Key {key} not found in config: {self.config}")
        return self.config[key]
    def read_or_throw_nested(self, keys):
        current = self.config
        for key in keys:
            if key not in current:
                raise ValueError(f"Key {key} not found in config: {current}")
            current = current[key]
        return current
    
    def load_program_args(self):
        args = self.read_or_throw_nested(["program-args", self.workload])
        res = ' '.join([f"--{key} {value}" for key, value in args.items()])
        return res
    def load_source_metrics(self):
        return self.read_or_throw_nested(["source-metrics", self.workload])
    def load_kafka_producer_url(self):
        return self.read_or_throw("kafka-producer-url")
    def load_dull_scheduler_enabled(self):
        return self.read_or_throw_nested(["ablation", "dull-scheduler"])
    def load_fwc_state_size(self):
        program_args = self.read_or_throw_nested(["program-args", "fwc"])
        state_size_bytes = program_args["stateSizeMean"]
        # convert to MB
        state_size_mb = (state_size_bytes / 1024) / 1024
        # keep 2 decimal places
        state_size_mb_str = "{:.2f}".format(state_size_mb)
        return state_size_mb_str + "MB"
    def load_fwc_input_rate(self):
        program_args = self.read_or_throw_nested(["program-args", "fwc"])
        return program_args["inputRate"]
    def load_fwc_skewness(self):
        program_args = self.read_or_throw_nested(["program-args", "fwc"])
        return program_args["keySkewFactor"]





def get_adj_matrix(case):
    return np.array(config["adjacency_matrix"][case]["m"]), config["adjacency_matrix"][case]["id_table"]


if __name__ == "__main__":
    config = ConfigGateway("drrs", "q8")