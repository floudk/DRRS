import json
import requests
from config_gateway import ConfigGateway
import time
import logging
import signal
import sys

class RestfulGateway:
    def __init__(self, config_gateway:ConfigGateway):
        self.master_url = config_gateway.master_url
        self.config_gateway = config_gateway

        self.restful_statistics = {}

        ################ drrs specific ################
        self.drrs_latency_request_params = None
        self.upstream_operators = None
    
    
    def request_with_throw(self, url: str, method: str, expected_status_code, print_error, **kwargs) -> dict:
        """
        return the response data of the request as json
        """
        response = requests.request(method, url, **kwargs)
        if response.status_code != expected_status_code:
            if print_error:
                print(response.json())
            raise ValueError(f"Request failed: {response.status_code}")
        return response.json()

    def wait_for_master_available(self, retries=8, backoff_factor=5):
        url = self.master_url + "/jobs"
        for i in range(retries):
            try:
                self.request_with_throw(url, "GET", 200, False, timeout=10)
                return
            except Exception as e:
                sleep_time = backoff_factor * (i + 1)
                print(f"Failed to connect to the cluster, retrying in {sleep_time} seconds")
                time.sleep(sleep_time)
    
    
    def upload_jar_and_wait_for_cluster_available(self):
        upload_url = self.master_url + "/jars/upload"
        taskmanagers_url = self.master_url + "/taskmanagers"

        with open(self.config_gateway.jar_path, 'rb') as jar_file:
            files = {'jarfile': ('flink-job.jar', jar_file, 'application/x-java-archive')}
            response_data = self.request_with_throw(upload_url, "POST", 200, True, files=files)
        self.jar_id = response_data.get("filename").split("/")[-1]
        print("Successfully uploaded jar(" + self.config_gateway.jar_path + ") and got jar id: " + self.jar_id)
        time.sleep(1)
        all_taskmanagers = self.config_gateway.taskmanager_number
        while True:
            response_data = self.request_with_throw(taskmanagers_url, "GET", 200, True)
            taskmanagers = response_data.get("taskmanagers")
            if len(taskmanagers) == all_taskmanagers:
                break
            print(f"Waiting for all taskmanagers to be ready {len(taskmanagers)}/{all_taskmanagers}")
            time.sleep(5)
    
    def get_now(self):
        url = self.master_url + "/jobs" + f"/{self.job_id}"
        response_data = self.request_with_throw(url, "GET", 200, True)
        timestamp = response_data.get("now", -1)
        if timestamp == -1:
            raise ValueError("Failed to get job timestamp")
        return timestamp

    def submit_job_and_wait_for_running(self):
        url = self.master_url + "/jars" + f"/{self.jar_id}" + "/run"
        payload = {
                "programArgs": self.config_gateway.load_program_args(),
                "parallelism": self.config_gateway.old_parallelism
                }
        response_data = self.request_with_throw(url, "POST", 200, True, data=json.dumps(payload), headers={"Content-Type": "application/json"})
        self.job_id = response_data.get("jobid", None)
        if self.job_id is None:
            print(response_data)
            raise ValueError("Failed to get job id")
        print("Running jar(" + self.jar_id + ") and got job id: " + self.job_id)
        self.restful_statistics["job_start_time"] = self.get_now()

        job_running_url = self.master_url + "/jobs" + f"/{self.job_id}"
        while True:
            response_json = self.request_with_throw(job_running_url, "GET", 200, True)
            state = response_json.get("state", None)
            if state is None:
                raise ValueError("Failed to get job state")
            if state == "RUNNING":
                break
            print("Waiting for job to be running...")
            time.sleep(0.5)


    def get_input_metrics(self):
        res = []
        url = self.master_url + "/jobs" + f"/{self.job_id}"
        response_data = self.request_with_throw(url, "GET", 200, True)
        vertices = response_data.get('vertices', [])
        source_mlist = self.config_gateway.load_source_metrics()
        for vertex in vertices:
            if 'source' in vertex.get('name').lower():
                label = vertex.get('name').split(":")[1].split(" ")[1]
                mname = []
                for m in source_mlist:
                    if label in m:
                        mname.append(m)
                res.append((vertex.get('parallelism'), vertex.get('id'), mname))
            elif vertex.get('name') == self.config_gateway.scaling_vertex_name:
                self.vertex_id = vertex.get('id')
            else:
                print("vertex name: ", vertex.get('name'), self.config_gateway.scaling_vertex_name)
        return res
    def init_vertex_id_only(self):
        url = self.master_url + "/jobs" + f"/{self.job_id}"
        response_data = self.request_with_throw(url, "GET", 200, True)
        vertices = response_data.get('vertices', [])
        for vertex in vertices:
            if vertex.get('name') == self.config_gateway.scaling_vertex_name:
                self.vertex_id = vertex.get('id')
                break
        if self.vertex_id is None:
            raise ValueError("Failed to get vertex id")

    def notify_kafka_producer_start(self):
        url = self.config_gateway.load_kafka_producer_url()
        r = requests.post(url)
        if r.status_code == 200:
            print("Successfully started kafka producer...")
        else:
            raise ValueError("Failed to start kafka producer")
    

    def wait_for_scaling(self, with_state_init=False, pre_time=7):
        
        url = self.master_url + "/jobs" + f"/{self.job_id}"+ "/scale"
        time_before_scale = self.config_gateway.time_before_scale
        payload = {
            "operator-name": self.config_gateway.scaling_vertex_name,
            "new-parallelism": self.config_gateway.new_parallelism
        }
        print("Expect to scale to parallelism: ", self.config_gateway.new_parallelism, " after ", time_before_scale, " seconds")
        
        if with_state_init:
            start_time = time.time()
            time.sleep(time_before_scale - pre_time)
            print("Initiating state size collection...")
            state_size_url = self.master_url + "/jobs" + f"/{self.job_id}" + "/scale" + f"/{self.vertex_id}" + "/statesize"
            response = self.request_with_throw(state_size_url, "GET", 200, True)

            # wait for the state size collection to finish
            # while response.get("status", {}).get("id", None) != "COMPLETED":
            #     print("Waiting for state size collection to finish...")
            #     time.sleep(0.2)
            #     response = self.request_with_throw(state_size_url, "GET", 200, True)
            # print("State size collection at ", time_before_scale-pre_time, " seconds", " finished: ", response.get("operation", {}).get("status", None))

            remaining_time = time_before_scale - (time.time() - start_time)
            if remaining_time > 0:
                time.sleep(remaining_time)
        else:
            time.sleep(time_before_scale)
        response_data = self.request_with_throw(url, "POST", 202, True, data=json.dumps(payload), headers={"Content-Type": "application/json"})
        self.trigger_id = response_data.get("request-id")
        if self.trigger_id is None:
            raise ValueError("Failed to trigger scale process")
        self.restful_statistics["scale_time"] = self.get_now()
        self.local_scale_time = time.time()
    
    def get_state_size_with_blocking(self):
        url = self.master_url + "/jobs" + f"/{self.job_id}" + "/scale" + f"/{self.vertex_id}" + "/statesize"
        response_data = self.request_with_throw(url, "GET", 200, True)
        while response_data.get("status", {}).get("id", None) != "COMPLETED":
            print("Waiting for state size collection...")
            time.sleep(0.2)
            response_data = self.request_with_throw(url, "GET", 200, True)
        state_size = response_data["operation"]["keyGroupStateSize"]
        if state_size is None:
            print(response_data)
            raise ValueError("Failed to get state size")
        return state_size
        
    def get_scale_metrics(self):
        url = self.master_url + "/jobs" + f"/{self.job_id}" + "/scale" + f"/{self.trigger_id}"
        response_data = self.request_with_throw(url, "GET", 200, True)
        state = response_data.get("status", {}).get("id", None)
        if state is None or state != "COMPLETED":
            raise ValueError("Failed to get scale status with state: ", state)
        return response_data.get("operation", {})

    
    def wait_for_cluster_update_completion(self):
        assert self.config_gateway.mechanism == "drrs"
        url = self.master_url + "/jobs" + f"/{self.job_id}" + "/scale" + f"/{self.trigger_id}"
        current_time = time.time()
        while True:
            response_data = self.request_with_throw(url, "GET", 200, True)
            state = response_data.get("status", {}).get("id", None)
            if state == "IN_PROGRESS":
                print("Waiting for cluster update completion...")
                time.sleep(0.2)
                continue
            print("Cluster update completed using ", time.time()-current_time, " seconds")
            break

    def is_reach_canceling_point(self):
        assert self.config_gateway.mechanism == "drrs" and self.config_gateway.workload == "fwc"    
        rest_time = self.config_gateway.time_after_scale - (time.time() - self.local_scale_time)
        return rest_time <= 0

    def wait_for_canceling(self, metric_collector):
        
        if self.config_gateway.do_not_scale:
            rest_time = self.config_gateway.time_after_scale + self.config_gateway.time_before_scale
            print("Cancel job after {} seconds...".format(rest_time))
            time.sleep(rest_time)
            metric_collector.stop_collecting()
            url = self.master_url + "/jobs" + f"/{self.job_id}" + "/yarn-cancel"
            self.request_with_throw(url, "GET", 202, True)
            self.restful_statistics["no_scale"] = True
        else:
            print(time.time() - self.local_scale_time)
            rest_time = self.config_gateway.time_after_scale - (time.time() - self.local_scale_time)
            print("Cancel job after {} seconds...".format(rest_time))
            if rest_time > 0:
                time.sleep(rest_time)
            metric_collector.stop_collecting()
            url = self.master_url + "/jobs" + f"/{self.job_id}" + "/yarn-cancel"
            self.request_with_throw(url, "GET", 202, True)


        url = self.master_url + "/jobs" + f"/{self.job_id}" + "/vertices" + f"/{self.vertex_id}"
        response_data = self.request_with_throw(url, "GET", 200, True)
        
        # get taskmanager ids and host info
        ids = {}
        hosts = {}
        for subtask in response_data.get('subtasks', []):
            subtask_id = subtask.get('subtask')
            taskmanagerid = subtask.get('taskmanager-id')
            ids[subtask_id] = taskmanagerid
            hosts[subtask_id] = subtask.get('host')
        res = {}
        for subtask_id, host in hosts.items():
            assert subtask_id in ids
            res[subtask_id] = (host, ids[subtask_id])
        self.restful_statistics["host_info"] = res
    
    def trigger_subscale(self, subscale):
        url = self.master_url + "/jobs" + f"/{self.job_id}" + "/scale" + f"/{self.trigger_id}" + "/subscale"
        payload = {
            "keys": subscale
        }
        response = requests.post(
            url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"}
        )
        response_data = response.json()
        subscale_id = response_data.get("request-id")
        if subscale_id is None or subscale_id != self.trigger_id:
            print(response_data)
            raise ValueError("Failed to trigger subscale")
        if "subscale_time" not in self.restful_statistics:
            self.restful_statistics["subscale_time"] = []
        self.restful_statistics["subscale_time"].append(self.get_now())
    
    def complete_scale(self):
        self.trigger_subscale([])
    
    def get_vertex_metrics(self,vertex_id,metrics):
        url = self.master_url + "/jobs" + f"/{self.job_id}" + "/vertices" + f"/{vertex_id}" + "/metrics"
        params = {}
        if metrics:
            params['get'] = ','.join(metrics)
        response = requests.get(url, params=params)
        response_data = response.json()
        if response.status_code != 200:
            raise ValueError("Failed to get vertex metrics")
        return response_data
    
    def init_drrs(self, latency_metrics):
        parallelism = self.get_sink_parallelism()
        print("Sink parallelism: ", parallelism)

        self.drrs_latency_request_params = {}
        metrics = []
        for metric in latency_metrics:
            for i in range(parallelism):
                new_metric = metric.replace("subtask", str(i))
                metrics.append(new_metric)
        self.drrs_latency_request_params["get"] = ",".join(metrics)
        print("Initiated DRRS with metrics: ", self.drrs_latency_request_params)
                
    def get_latencies(self):
        url = self.master_url + "/jobs" + f"/{self.job_id}" + "/metrics"
        response = self.request_with_throw(url, "GET", 200, True, params=self.drrs_latency_request_params)
        # print(response)
        return response
    
    def get_sink_parallelism(self):
        """
        since sink operator is special for latency retrieval, 
        we need to get the parallelism of sink operator separately.
        """
        sink_operator_name = "FileSink: Writer"

        url = self.master_url + "/jobs" + f"/{self.job_id}"
        response = self.request_with_throw(url, "GET", 200, True)
        vertices = response.get('vertices', [])
        for vertex in vertices:
            if vertex.get('name') == sink_operator_name:
                return vertex.get('parallelism')
        raise ValueError("Failed to get sink parallelism")

    def set_upstream_operators(self,operators):
        self.upstream_operators = operators
    def get_backpressure(self):
        url = self.master_url + "/jobs" + f"/{self.job_id}" 
        response = self.request_with_throw(url, "GET", 200, True)
        vertices = response.get('vertices', [])
        for vertex in vertices:
            if self.upstream_operators in vertex.get('name'):
                logging.info("Upstream operator metrics: "+str(vertex["metrics"]))
                return vertex["metrics"]["accumulated-backpressured-time"]