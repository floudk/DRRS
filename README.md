# Fine-Grained Dynamic Scaling for Stateful Stream Processing Systems

# **⚠️ Important Notice**:  
We are currently organizing the code related to automation scripts and baseline implementations.  We guarantee that this work will be completed within two days after the ICDE Revision Due on **February 27, 2025 (11:59 PM AoE)**.  
If you are accessing this repository before that time, we kindly ask for your patience.

## Overview
This repository presents **DRRS**, a novel on-the-fly dynamic scaling mechanism designed to achieve fine-grained scalability with minimal performance degradation in stateful stream processing systems during scaling events.

DRRS is implemented as a research prototype on top of [Apache Flink 1.17.0](https://github.com/apache/flink/tree/release-1.17.0-rc3).

## Prerequisites
All pre-requisites are the same as the original Flink, please refer to Flink's official documentation for more details.

## Installation

We offer two installation methods: using the provided Docker image (recommended) or building from source.

### Docker Image
1. **Pull the Docker image**: To pull the Docker image from Docker Hub, run the following command:
    ```bash
    docker push floudk/drrs-experiment-suite:latest
    ```
### Building from Source
1. **Build the Original Flink**: DRRS is built on top of Apache Flink, so it is recommended to first build the base Flink source code by following the official instructions. This will ensure that Maven and other required dependencies are configured correctly. Building Flink first will also enable the use of Maven’s incremental build feature, as DRRS modifies a limited set of modules.
2. **Clone the repository**: Clone this repository and replace the relevant modules and files in the original Flink source code with those found in the `DRRS-on-Flink`.
3. **Build from source**: Use Maven to build the project. As DRRS is a research prototype, you may need to bypass some checks and validations. To do this, remove certain plugins in the `pom.xml` (see `DRRS-on-Flink/pom.xml`) and use the additional Maven flags as shown below to build the modified modules.(Make sure you’ve built Flink first):
    ```bash
    mvn clean install -pl flink-runtime,flink-streaming-java,flink-examples,flink-core,flink-dist  -DskipTests=true -Dlicense.skip=true -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Dcheckstyle.failOnViolation=false -Dcheckstyle.module.EmptyLineSeparator.severity=ignore
    mvn clean install -pl flink-clients,flink-dist -DskipTests=true -Dlicense.skip=true -Dcheckstyle.failOnViolation=false -Dcheckstyle.module.EmptyLineSeparator.severity=ignore
    ```
4. **Configure Flink**: Similar to setting up the original Flink environment, you'll need to configure Flink by updating configuration files, adding required JARs, and making any other necessary adjustments. For detailed instructions, also refer to Flink's official documentation.
5. **Configure DRRS**: After building, move the `scale-conf.properties` file to the `conf` directory of your Flink build target directory.

## Evaluation
The evaluation of DRRS is conducted using the NexMark benchmark and a real-world Twitch workload. We provide a set of scripts and Docker Compose files to facilitate the deployment of the Flink cluster and the execution of experiments. Detailed instructions can be found in the `evaluation` directory.

## Baseline Implementations
For fair comparison and reproducibility, we provide baseline implementations, including Meces and Megaphone, using the same Flink version. These implementations are located in the `baselines` directory.   
Similar to DRRS, both Docker image and source-building methods are provided (following the same steps as DRRS).

## Contact
For questions or clarifications, please either create an issue on the repository or contact the authors directly.

## Paper
Towards Fine-Grained Scalability for Stateful Stream Processing Systems
[Publication details to be updated]

## License
This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## ⚠️ Disclaimer

This project is an academic prototype, and it is not intended to be used in production environments. Some limitations include but are not limited to:
- No comprehensive tests: The implementation does not include unit tests or integration tests.
- Code style deviations: The code may not fully adhere to common coding standards such as Checkstyle, and some JavaDocs may be incomplete or missing.

**More details will be updated upon publication.**
