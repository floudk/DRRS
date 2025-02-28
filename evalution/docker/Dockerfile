FROM eclipse-temurin:11-jre-jammy

# Install dependencies
RUN set -ex; \
  sed -i 's/http:\/\/archive.ubuntu.com/https:\/\/mirror.sjtu.edu.cn/g' /etc/apt/sources.list; \
  apt-get update; \
  apt-get -y install gpg libsnappy1v5 gettext-base libjemalloc-dev; \
  rm -rf /var/lib/apt/lists/*

# ENV GOSU_VERSION 1.11
# wget -nv -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)"; \
# wget -nv -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc"; \
COPY path/to/gosu-amd64 /usr/local/bin/gosu
COPY path/to/gosu-amd64.asc /usr/local/bin/gosu.asc

# Grab gosu for easy step-down from root

RUN set -ex; \
  export GNUPGHOME="$(mktemp -d)"; \
  for server in ha.pool.sks-keyservers.net $(shuf -e \
                          hkp://p80.pool.sks-keyservers.net:80 \
                          keyserver.ubuntu.com \
                          hkp://keyserver.ubuntu.com:80 \
                          pgp.mit.edu) ; do \
      gpg --batch --keyserver "$server" --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 && break || : ; \
  done && \
  gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
  gpgconf --kill all; \
  rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
  chmod +x /usr/local/bin/gosu; \
  gosu nobody true

COPY path/to/DRRS/flink-dist/target/flink-1.17.0-bin/flink-1.17.0/bin /opt/flink/bin
COPY path/to/DRRS/flink-dist/target/flink-1.17.0-bin/flink-1.17.0/conf /opt/flink/conf
COPY path/to/DRRS/flink-dist/target/flink-1.17.0-bin/flink-1.17.0/lib /opt/flink/lib
COPY path/to/DRRS/flink-dist/target/flink-1.17.0-bin/flink-1.17.0/opt /opt/flink/opt
COPY path/to/DRRS/flink-dist/target/flink-1.17.0-bin/flink-1.17.0/plugins /opt/flink/plugins

COPY path/to/kafka-clients-3.8.0.jar /opt/flink/lib/kafka-clients-3.8.0.jar
COPY path/to/flink-connector-kafka-1.17.0.jar /opt/flink/lib/flink-connector-kafka-1.17.0.jar

RUN mkdir -p /opt/flink/log

RUN mkdir -p /opt/output
RUN chmod -R 777 /opt/output

# delete backup files in the workdir to assure the correctness.
RUN rm -rf /opt/flink/lib/flink-dist-1.17.0.jar

# copy all the jars in the lib folder to the backup folder, so that we can choose which jars to use in runtime
# drrs
RUN mkdir -p /opt/bak/drrs
COPY path/to/DRRS/flink-dist/target/flink-1.17.0-bin/flink-1.17.0/lib/flink-dist-1.17.0.jar /opt/bak/drrs/flink-dist-1.17.0.jar

# meces
RUN mkdir -p /opt/bak/meces
COPY path/to/Meces-on-Flink/flink-dist/target/flink-1.17.0-bin/flink-1.17.0/lib/flink-dist-1.17.0.jar /opt/bak/meces/flink-dist-1.17.0.jar

# megaphone
RUN mkdir -p /opt/bak/megaphone
COPY path/to/Megaphone-on-Flink/flink-dist/target/flink-1.17.0-bin/flink-1.17.0/lib/flink-dist-1.17.0.jar /opt/bak/megaphone/flink-dist-1.17.0.jar



# Prepare environment
RUN groupadd --system --gid=9999 flink


COPY evaluation/docker/entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

RUN useradd --system --home-dir /opt/flink --uid 9999 --gid=flink flink
RUN chown -R flink:flink /opt/flink
ENTRYPOINT ["/docker-entrypoint.sh"]
EXPOSE 6123 8081