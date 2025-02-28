# Evaluation

This directory contains the scripts and Docker Compose files necessary for deploying the Flink cluster and executing experiments to evaluate DRRS.

The evaluation is conducted using two setups: a single-node Docker-based Flink cluster and a Docker Swarm-based multi-node Flink cluster. The single-node setup is used for controlled runtime to avoid interference that cannot be mitigated by system design mechanisms,  while the multi-node setup is used to assess the scalability of DRRS in a more realistic environment.


## Directory Structure
```
.
├── controller # Scale Planner as the external controller
├── datagen # External data generator for the NexMark and Twitch workloads
├── docker # Docker Compose files and scripts for evaluation
├── evaluate.sh # Main script for running the evaluation
├── helper.sh # Helper functions for the evaluation
├── README.md
└── test_jobs # jar files for all workloads
.
```

## Prerequisites
1. **Docker & Docker Compose**: Ensure Docker and Docker Compose (v24.0.2 recommended) are installed in all machines where the Flink cluster will run, as all experiments are run in Docker containers.
2. **Python 3.x**: The external controller (*Scale Planner*, as mentioned in the paper) is implemented in Python. You will need Python 3.x installed (Python 3.9 recommended), along with the required dependencies (see `controller/requirements.txt`).
3. **Flink Image**: The image `floudk/drrs-experiment-suite:latest` must be available on all machines where the Flink cluster will run. If building from source, you need to build the image after building Flink and tag it as `floudk/drrs-experiment-suite:latest`. Related `Dockerfile` and `entrypoint.sh` files are provided in the `docker` directory. Use `docker tag` to rename the image to `floudk/drrs-experiment-suite:latest` if necessary.
4. **Test Jobs**: For users using the pre-built image, the test jobs can be found in the `test_jobs` directory. For users building from source, the JAR files located in the `/path/to/build-target/examples/streaming` directory can also be used. These JAR files are required for submitting the test jobs to the Flink cluster and must be available on the machine running the controller.
5. **Set Path**: Ensure all paths are correctly configured in the `helper.sh`, `evaluate.sh`, and `docker/docker-compose.*.yaml` files.  If additional machines are used, ensure the SSH username and IP addresses are correctly set in the `docker/docker-compose.kafka.yaml` file and the `helper.sh` file. Some other paths also need to be updated in the `controller/*.py` and `controller/conf/config.json` files to better suit your environment, like log paths and Flink REST API URLs. (You may find a global search for `/path/to/` useful to locate all paths that need to be updated.)


### Specific Prerequisites For Single-Node Setup

For the single-node setup, we provide three different workloads: NexMark Query 7, NexMark Query 8, and the Twitch workload. 

To ensure the stability of the measurements, we recommend running the input generator on a separate machine to avoid interference with the Flink cluster.
The`docker/docker-compose.kafka.yaml` file is used to generate the input from separate machines. Docker and the related images must be available on the machine running the Kafka cluster. Additionally, the `datagen` directory must be accessible on the machine running the data generator to ensure the `volume` in the `docker/docker-compose.kafka.yaml` can be mounted correctly.

Additionally, for Twitch workload, the data should be first installed from [LiveRec](https://github.com/JRappaz/liverec), then use the `datagen/twitch_preprocessing.ipynb` to preprocess the data and generate the input for the Twitch workload.

### Specific Prerequisites For Docker Swarm Setup

First, set up a Docker Swarm cluster by running `docker swarm init` on the manager node and `docker swarm join` on the worker nodes. For more details, refer to the official Docker documentation.

Since the config file `scale-conf.properties` is used to configure the Flink cluster, it must be available on all machines where the Flink cluster will run. This can be achieved with the Swarm command `docker config create scale-conf /path/to/scale-conf.properties` and then attach it to the service in the `docker/docker-compose.swarm.yaml` file.

## Running the Evaluation

### single-node experiment
```bash
./evaluate.sh evaluate <prototype>[drrs|meces|megaphone] <workload>[q7|q8|twitch]
```

### multi-node experiment
```bash
# set_sensitive_analysis_config in evaluate.sh to set the desired configurations for fwc sensitivity analysis
./evaluate.sh swarm <prototype>[drrs|meces|megaphone] <workload>[fwc]
```

To stop the running experiments, use:
```bash
./evaluate.sh stop
```

After completing the evaluation, results will be saved in the `results` directory (or any other specified directory in your `evaluate.sh` script).
We also provide a Jupyter notebook (`results_analysis.ipynb`) to help you extract the important metrics for any further analysis and visualization.


For any questions or clarifications, we are happy to help. Please either create an issue on the repository or contact the authors directly.
