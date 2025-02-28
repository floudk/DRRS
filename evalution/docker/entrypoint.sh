#!/usr/bin/env bash

###############system choose################
SYSTEM_TYPE=${SYSTEM_TYPE:-drrs}  # drrs(default), meces, megaphone, unbound, otfs

echo "Run: $SYSTEM_TYPE"
cp /opt/bak/$SYSTEM_TYPE/flink-dist-1.17.0.jar /opt/flink/lib/flink-dist-1.17.0.jar


FLINK_HOME=${FLINK_HOME:-/opt/flink}
export PATH=$FLINK_HOME/bin:$PATH

echo "FLINK_HOME: $FLINK_HOME"

sed -i 's/rest.address: localhost/rest.address: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
sed -i 's/rest.bind-address: localhost/rest.bind-address: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
sed -i 's/jobmanager.bind-host: localhost/jobmanager.bind-host: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
sed -i 's/taskmanager.bind-host: localhost/taskmanager.bind-host: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
sed -i '/taskmanager.host: localhost/d' $FLINK_HOME/conf/flink-conf.yaml;

JOB_MANAGER_RPC_ADDRESS=${JOB_MANAGER_RPC_ADDRESS:-$(hostname -f)}
CONF_FILE="${FLINK_HOME}/conf/flink-conf.yaml"


drop_privs_cmd() {
    if [ $(id -u) != 0 ]; then
        # Don't need to drop privs if EUID != 0
        return
    elif [ -x /sbin/su-exec ]; then
        # Alpine
        echo su-exec flink
    else
        # Others
        echo gosu flink
    fi
}

set_config_option() {
  local option=$1
  local value=$2

  # escape periods for usage in regular expressions
  local escaped_option=$(echo ${option} | sed -e "s/\./\\\./g")

  # either override an existing entry, or append a new one
  if grep -E "^${escaped_option}:.*" "${CONF_FILE}" > /dev/null; then
        sed -i -e "s/${escaped_option}:.*/$option: $value/g" "${CONF_FILE}"
  else
        echo "${option}: ${value}" >> "${CONF_FILE}"
  fi
}

prepare_configuration() {
    set_config_option jobmanager.rpc.address ${JOB_MANAGER_RPC_ADDRESS}
    set_config_option blob.server.port 6124
    set_config_option query.server.port 6125
    set_config_option akka.ask.timeout 20s
    set_config_option metrics.latency.history-size 64
    

    if [ -n "${FLINK_PROPERTIES}" ]; then
        echo "${FLINK_PROPERTIES}" | while IFS= read -r line; do

            option=$(echo "$line" | cut -d':' -f1)
            value=$(echo "$line" | cut -d':' -f2- | xargs)
            # if option not empty
            if [ -n "$option" ]; then
                echo "Setting $option to $value"
                set_config_option "$option" "$value"
            fi

        done
    fi
    envsubst < "${CONF_FILE}" > "${CONF_FILE}.tmp" && mv "${CONF_FILE}.tmp" "${CONF_FILE}"
}


maybe_enable_jemalloc() {
    if [ "${DISABLE_JEMALLOC:-false}" == "false" ]; then
        JEMALLOC_PATH="/usr/lib/$(uname -m)-linux-gnu/libjemalloc.so"
        JEMALLOC_FALLBACK="/usr/lib/x86_64-linux-gnu/libjemalloc.so"
        if [ -f "$JEMALLOC_PATH" ]; then
            export LD_PRELOAD=$LD_PRELOAD:$JEMALLOC_PATH
        elif [ -f "$JEMALLOC_FALLBACK" ]; then
            export LD_PRELOAD=$LD_PRELOAD:$JEMALLOC_FALLBACK
        else
            if [ "$JEMALLOC_PATH" = "$JEMALLOC_FALLBACK" ]; then
                MSG_PATH=$JEMALLOC_PATH
            else
                MSG_PATH="$JEMALLOC_PATH and $JEMALLOC_FALLBACK"
            fi
            echo "WARNING: attempted to load jemalloc from $MSG_PATH but the library couldn't be found. glibc will be used instead."
        fi
    fi
}

maybe_enable_jemalloc
prepare_configuration

cat conf/flink-conf.yaml | grep jobmanager

args=("$@")
if [ "$1" = "jobmanager" ]; then
    args=("${args[@]:1}")

    echo "Starting Job Manager"

    exec $(drop_privs_cmd) "$FLINK_HOME/bin/jobmanager.sh" start-foreground "${args[@]}"
elif [ "$1" = "taskmanager" ]; then
    args=("${args[@]:1}")

    echo "Starting Task Manager"

    exec $(drop_privs_cmd) "$FLINK_HOME/bin/taskmanager.sh" start-foreground "${args[@]}"
fi

args=("${args[@]}")

# Running command in pass-through mode
exec $(drop_privs_cmd) "${args[@]}"
