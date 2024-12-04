# Fine-Grained Dynamic Scaling for Stateful Stream Processing Systems

## Overview
This repository presents **DRRS**, a novel on-the-fly dynamic scaling mechanism designed to achieve fine-grained scalability with minimal performance degradation in stateful stream processing systems during scaling events.

DRRS is implemented as a research prototype on top of [Apache Flink 1.17.0](https://github.com/apache/flink/tree/release-1.17.0-rc3).

## Prerequisites
All pre-requisites are the same as the original Flink, please refer to Flink's official documentation for more details.

## Installation
1. **Clone the repository**: Clone this repository and replace all the relevant modules and files in the original Flink source code with the ones provided in directory `DRRS-on-Flink`.
2. **Build from source**: Use Maven to build the project. Since this is a research prototype, the build process requires bypassing several checks and validations. This involves removing certain plugins in the `pom.xml` and using additional maven flags `-DskipTests=true -Dlicense.skip=true -Dmaven.javadoc.skip=true -Dcheckstyle.failOnViolation=false -Dcheckstyle.module.EmptyLineSeparator.severity=ignore`.
3. **Configure Flink**: Similar to setting up the original Flink environment, you'll need to configure Flink by updating configuration files, adding required JARs, and making any other necessary adjustments. For detailed instructions, also refer to Flink's official documentation.
4. **Configure DRRS**: After building, move the `scale-conf.properties` file to the `conf` directory of your Flink build target directory.

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