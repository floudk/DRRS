#!/bin/bash
# make the parent directory of the script directory the current directory
PROJECT_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"
COMPILE_ENV_FILE="$PROJECT_HOME/.last_compilation"

EXTERNAL_PY_CONTROLLER="/path/to/controller/controller.py"

SSH_USER="SSH_USER"

DOCKER_SCRIPT="/path/to/evaluation/helper.sh"
LOG_DIR="/path/to/evaluation/results"

SCALE_CONF="/path/to/evaluation/docker/scale-conf.properties"
CONTROLLER_CONFIG="/path/to/controller/conf/config.json"

DATA_GEN_DIR="/path/to/evaluation/datagen"
GEN_WORKER="smile3"
WORKER_GEN_DIR="/path/to/nexmark_gen"
NEXMARK_YAML="/path/to/evaluation/datagen/nexmark.yaml"
DOCKER_COMPOSE_KAFKA_FILE="/path/to/evaluation/docker/docker-compose.kafka.yaml"

DRRS_SCHEDULER_CONF="/path/to/controller/conf/drrs.schedule.json"


start_cluster(){
    . $DOCKER_SCRIPT $1 $2 $3
}

stop_standalone_cluster(){
   . $DOCKER_SCRIPT standalone stop
   ssh $SSH_USER@$GEN_WORKER "cd $WORKER_GEN_DIR; docker compose down"
}

stop_swarm_cluster(){
    . $DOCKER_SCRIPT swarm stop
    # no need to use docker stack rm $SWARM_STACK_NAME
}
stop_clusters(){
    stop_standalone_cluster
    stop_swarm_cluster
}


drrs_update_intra_subscale_scheduler(){
    intra_subscale_scheduler=$(jq -r ".\"intra-scheduler\"" $DRRS_SCHEDULER_CONF)
    echo "update intra-scheduler to $intra_subscale_scheduler"
    if [ "$intra_subscale_scheduler" = "lex" ]; then
        # comment out the line with AdaptiveHeuristic if needed
        sed -i "s|^\(drrs.subscale.internal-key-scheduler=AdaptiveHeuristic\)$|# \1|g" $SCALE_CONF
        # uncomment the line with Lexicographic if needed
        sed -i "s|^# \(drrs.subscale.internal-key-scheduler=Lexicographic\)$|\1|g" $SCALE_CONF
    elif [ "$intra_subscale_scheduler" = "adaptive" ]; then
        # comment out the line with Lexicographic if needed
        sed -i "s|^\(drrs.subscale.internal-key-scheduler=Lexicographic\)$|# \1|g" $SCALE_CONF
        # uncomment the line with AdaptiveHeuristic if needed
        sed -i "s|^# \(drrs.subscale.internal-key-scheduler=AdaptiveHeuristic\)$|\1|g" $SCALE_CONF
    else
        echo "intra-scheduler: $intra_subscale_scheduler is not supported!"
        exit 1
    fi
}

drrs_set_migration_buffer_capacity(){
    capacity=$1

    sed -i "s|drrs.cache.capacity=[0-9]\{1,\}|drrs.cache.capacity=$capacity|g" $SCALE_CONF
    # assert proper cache capacity
    cache_capacity_value=$(grep -E "^drrs.cache.capacity=" $SCALE_CONF | awk -F= '{print $2}')
    if [ "$cache_capacity_value" != "$capacity" ]; then
        echo "drrs.cache.capacity is not $capacity"
        exit 1
    fi
}


set_sensitive_analysis_config(){
    state_size=$1
    input_rate=$2
    skewness=$3
    echo "set stateSize to $state_size, inputRate to $input_rate, keySkewFactor to $skewness"
    
    sed -i "s|\"stateSizeMean\": [0-9]\{1,\}|\"stateSizeMean\": $state_size|g" $CONTROLLER_CONFIG
    sed -i "s|\"inputRate\": [0-9]\{1,\}|\"inputRate\": $input_rate|g" $CONTROLLER_CONFIG
    sed -i "s|\"keySkewFactor\": [0-9]\{1,\}.[0-9]\{1,\}|\"keySkewFactor\": $skewness|g" $CONTROLLER_CONFIG
}

RECOMPILE(){
    src_home=$1
    
    # try read last_recompile_home from $COMPILE_ENV_FILE
    if [ -f "$COMPILE_ENV_FILE" ]; then
        source $COMPILE_ENV_FILE
    fi

    stop_clusters

    # if LAST_RECOMPILE_HOME is set and not equal to src_home, then warn the user
    if [ -n "$LAST_RECOMPILE_HOME" ] && [ "$LAST_RECOMPILE_HOME" != "$src_home" ]; then
        echo "The last recompile home is $LAST_RECOMPILE_HOME, not equal to the current recompile home $src_home"
        echo "Recompile flink-runtime, flink-streaming-java, flink-examples, flink-core, flink-clients, flink-dist to replace the old maven cache"

        # need to re-compile all changed modules
        cd $src_home; mvn clean install -pl flink-runtime,flink-streaming-java,flink-examples,flink-core,flink-dist  -DskipTests=true -Dlicense.skip=true -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Dcheckstyle.failOnViolation=false -Dcheckstyle.module.EmptyLineSeparator.severity=ignore
        
        # if last failed, exit
        if [ $? -eq 1 ]; then
            echo "Recompile failed, please check the error message!"
            exit 1
        fi

        if [[ $2 == *"c"* ]]; then
            cd $src_home; mvn install -pl flink-clients,flink-dist -DskipTests=true -Dlicense.skip=true -Dcheckstyle.failOnViolation=false -Dcheckstyle.module.EmptyLineSeparator.severity=ignore
            if [ $? -eq 1 ]; then
                echo "Recompile failed, please check the error message!"
                exit 1
            fi
        fi

        # re-write the COMPILE_ENV_FILE
        echo "LAST_RECOMPILE_HOME=$src_home" > $COMPILE_ENV_FILE
    fi


    MODULES="flink-dist"
    if [[ $2 == *"e"* ]]; then
        MODULES="flink-examples/flink-examples-streaming,$MODULES"
    fi
    if [[ $2 == *"r"* ]]; then
        MODULES="flink-runtime,$MODULES"
    fi
    if [[ $2 == *"s"* ]]; then
        MODULES="flink-streaming-java,$MODULES"
    fi
    cd $src_home; mvn clean install -pl $MODULES -DskipTests=true -Dlicense.skip=true -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Dcheckstyle.failOnViolation=false -Dcheckstyle.module.EmptyLineSeparator.severity=ignore
    if [[ $2 == *"c"* ]]; then
        # since flink-clients must use test, so -Dmaven.test.skip=true is not used
        # separately compile flink-clients and then compile the rest
        cd $src_home; mvn clean install -pl flink-clients,flink-dist -DskipTests=true -Dlicense.skip=true -Dcheckstyle.failOnViolation=false -Dcheckstyle.module.EmptyLineSeparator.severity=ignore
    fi
}

# check argument
if [ $# -eq 0 ]; then
    echo "Usage: $0 operation"
    exit 1
elif [ $1 = "cleanidea" ]; then
    $PROJECT_HOME/scripts/cleanRemoteIdea.sh
    exit 0
elif [ $1 = "evaluate" ]; then
    # assert $2 is in [unbound, megaphone, drrs, meces, otfs] and $3 is in [fwc, q7, q8, twitch]
    if [ $# -eq 3 ] && { [ $2 != "unbound" ] && [ $2 != "megaphone" ] && [ $2 != "drrs" ] && [ $2 != "meces" ] && [ $2 != "otfs" ]; } || { [ $3 != "fwc" ] && [ $3 != "q7" ] && [ $3 != "q8" ] && [ $3 != "twitch" ]; }; then
        echo "Usage: $0 evaluate mechanism[unbound|megaphone|drrs|meces|otfs] case[fwc|q7|q8|twitch]"
        exit 1
    fi

    stop_standalone_cluster

    # create LOG_DIR/tmp
    if [ -d "$LOG_DIR/tmp" ]; then
        # if only tmp/scheduler.log exists, then ignore
        # notcie only tmp/scheduler.log exists and on other files
        if [ $(ls $LOG_DIR/tmp | wc -l) -eq 1 ] && [ -f "$LOG_DIR/tmp/scheduler.log" ]; then
            echo "LOG_DIR/tmp exists, only scheduler.log exists, ignore..."
        else
            echo "LOG_DIR/tmp exists, rename it to error-timestamp(day-hour-minute)"
            timestamp=$(date +"%d-%H-%M")
            mv $LOG_DIR/tmp $LOG_DIR/error-$timestamp
        fi
    fi
    mkdir -p $LOG_DIR/tmp

    #################### non-fwc specific ####################

    if [ "$3" = "q7" ]; then
        echo "update nexmark.yaml for q7"
        # person_proportion: 1
        # auction_proportion: 3
        # bid_proportion: 46
        sed -i "s|person_proportion: [0-9]\{1,\}|person_proportion: 0|g" $NEXMARK_YAML
        sed -i "s|auction_proportion: [0-9]\{1,\}|auction_proportion: 0|g" $NEXMARK_YAML
        sed -i "s|bid_proportion: [0-9]\{1,\}|bid_proportion: 50|g" $NEXMARK_YAML
    elif [ "$3" = "q8" ]; then
        echo "update nexmark.yaml for q8"
        # person_proportion: 10
        # auction_proportion: 30
        # bid_proportion: 10
        sed -i "s|person_proportion: [0-9]\{1,\}|person_proportion: 20|g" $NEXMARK_YAML
        sed -i "s|auction_proportion: [0-9]\{1,\}|auction_proportion: 30|g" $NEXMARK_YAML
        sed -i "s|bid_proportion: [0-9]\{1,\}|bid_proportion: 0|g" $NEXMARK_YAML
    fi

    if [ "$3" != "fwc" ]; then
        echo "Starting kafka cluster..."
        # update docker-compose.yaml in smile 3 /path/to/nexmark_gen/docker-compose.yaml
        ssh $SSH_USER@$GEN_WORKER "sed -i 's|TOPIC: [a-zA-Z0-9]\{1,\}|TOPIC: $3|g' $WORKER_GEN_DIR/docker-compose.yaml"
        rsync -avz $DATA_GEN_DIR/ $SSH_USER@$GEN_WORKER:$WORKER_GEN_DIR/datagen/ > /dev/null
        ssh $SSH_USER@$GEN_WORKER "cd $WORKER_GEN_DIR; docker compose up -d"
    fi
    
    ##############################################

    # get replica factor from CONTROLLER_CONFIG
    replicas=$(jq -r ".taskmanagers.$3" $CONTROLLER_CONFIG)
    start_cluster standalone $2 $replicas


    # invoke EXTERNAL_PY_CONTROLLER
    python3 $EXTERNAL_PY_CONTROLLER $2 $3

    # download docker logs to local
    containers=$(docker ps --filter "name=docker-taskmanager" --format "{{.Names}}")
    for container in $containers
    do
        log_file="${LOG_DIR}/tmp/${container}.log"
        # echo "Downloading logs for $container to $log_file"
        docker logs "$container" > "$log_file" 2>&1

        if [ "$3" = "fwc" ]; then
            if docker exec -i "$container" bash -c "[ -d /opt/output ]"; then
                output_dirs=$(docker exec -i "$container" bash -c "ls -1 /opt/output")
                if [ -n "$output_dirs" ]; then
                    merged_file="/opt/output/merged_output.txt"
                    docker exec -i "$container" bash -c "find /opt/output/* -name 'part-*' -exec cat {} + >> $merged_file"
                    # download merged output file with no stdout
                    docker cp $container:$merged_file $LOG_DIR/tmp/output_${container}.txt > /dev/null
                fi
            fi
        fi
    done

    # if there exists output_*.txt files, then merge them into output.log
    if ls $LOG_DIR/tmp/output_*.txt 1> /dev/null 2>&1; then
        cat $LOG_DIR/tmp/output_*.txt > $LOG_DIR/tmp/output.log
        rm $LOG_DIR/tmp/output_*.txt
    fi

    # jobmanager logs
    docker logs "docker-jobmanager-1" > "${LOG_DIR}/tmp/jobmanager.log" 2>&1

    # rename tmp folder to: system-case-timestamp(day-hour-minute)
    timestamp=$(date +"%d-%H-%M")

    mv $LOG_DIR/tmp $LOG_DIR/$2-$3-$timestamp
    
    exit 0
elif [ $1 = "swarm" ]; then
    if [ $# -eq 3 ] && { [ $2 != "unbound" ] && [ $2 != "megaphone" ] && [ $2 != "drrs" ] && [ $2 != "meces" ] && [ $2 != "otfs" ]; } || [ $3 != "fwc" ]; then
        echo "Usage: $0 swarm mechanism[unbound|megaphone|drrs|meces|otfs] case[fwc], instead of $0 swarm $2 $3"
        exit 1
    fi
    stop_swarm_cluster

    # create LOG_DIR/tmp
    if [ -d "$LOG_DIR/tmp" ]; then
        echo "LOG_DIR/tmp exists, rename..."
        timestamp=$(date +"%d-%H-%M")
        mv $LOG_DIR/tmp $LOG_DIR/error-$timestamp
    fi
    mkdir -p $LOG_DIR/tmp

    # get replica factor from CONTROLLER_CONFIG
    replicas=$(jq -r ".taskmanagers.$3" $CONTROLLER_CONFIG)
    start_cluster swarm $2 $replicas

    # invoke EXTERNAL_PY_CONTROLLER
    # sleep 10s to wait for the jobmanager to be ready
    sleep 20s
    python3 $EXTERNAL_PY_CONTROLLER $2 $3

    echo "Downloading logs and output files to local..."

workers=("work1" "work2" "work3" "work4")

for worker in "${workers[@]}"; do
    (
        ssh -T "$SSH_USER@$worker" <<'EOF'
            for container in $(docker ps --filter "name=evaluation_taskmanager" --format "{{.Names}}"); do
                if docker exec "$container" bash -c '[ -d /opt/output ]' &>/dev/null; then
                    output_file="/tmp/flink/output_${container}.txt"
                    mkdir -p /tmp/flink/
                    if ! docker exec "$container" find /opt/output/ -type f \( -name "part-*" -o -name ".part-*.inprogress*" \) -exec cat {} + > "$output_file" 2>/dev/null; then
                        touch "$output_file"
                    fi
                fi
            done
EOF

        scp -q "$SSH_USER@$worker:/tmp/flink/output_*.txt" "$LOG_DIR/tmp/" 2>/dev/null || true

        ssh -T "$SSH_USER@$worker" "rm -rf /tmp/flink/" &>/dev/null
    ) & 
done

wait  

if ls "$LOG_DIR"/tmp/output_*.txt &>/dev/null; then
    grep -vh '^$' "$LOG_DIR"/tmp/output_*.txt > "$LOG_DIR/tmp/output.log"
    rm -f "$LOG_DIR"/tmp/output_*.txt
else
    echo "No output files found, please check the logs!" >&2
fi


    # jobmanager logs
    docker service logs evaluation_jobmanager > "${LOG_DIR}/tmp/jobmanager.log" 2>&1

    # rename tmp folder to: system-case-timestamp(day-hour-minute)
    timestamp=$(date +"%d-%H-%M")

    mv $LOG_DIR/tmp $LOG_DIR/swarm-$2-$3-$timestamp
    exit 0

elif [ $1 = "as" ]; then
   

    
    $0 stop
    workload="twitch"

    # study 1: no subscale division 
    # echo "study 1: no subscale division"
    # sed -i "s|\"dull-scheduler\": [a-z]\{1,\}|\"dull-scheduler\": true|g" $CONTROLLER_CONFIG
    # sed -i "s|drrs.enable-subscale=[a-z]\{1,\}|drrs.enable-subscale=false|g" $SCALE_CONF
    # $0 evaluate drrs q8
    # sed -i "s|\"dull-scheduler\": [a-z]\{1,\}|\"dull-scheduler\": false|g" $CONTROLLER_CONFIG
    # sed -i "s|drrs.enable-subscale=[a-z]\{1,\}|drrs.enable-subscale=true|g" $SCALE_CONF


    echo "study 1: only decoupling and rerouting"
    sed -i "s|\"dull-scheduler\": [a-z]\{1,\}|\"dull-scheduler\": true|g" $CONTROLLER_CONFIG
    sed -i "s|drrs.enable-subscale=[a-z]\{1,\}|drrs.enable-subscale=false|g" $SCALE_CONF
    sed -i "s|drrs.cache.capacity=[0-9]\{1,\}|drrs.cache.capacity=1|g" $SCALE_CONF
    $0 evaluate drrs $workload
    sed -i "s|\"dull-scheduler\": [a-z]\{1,\}|\"dull-scheduler\": false|g" $CONTROLLER_CONFIG
    sed -i "s|drrs.enable-subscale=[a-z]\{1,\}|drrs.enable-subscale=true|g" $SCALE_CONF
    sed -i "s|drrs.cache.capacity=[0-9]\{1,\}|drrs.cache.capacity=200|g" $SCALE_CONF

    # study 2: no reordering
    ## drrs.cache.capacity: 1
    
    # echo "study 2: no reordering"
    # sed -i "s|drrs.cache.capacity=[0-9]\{1,\}|drrs.cache.capacity=1|g" $SCALE_CONF
    # $0 evaluate drrs q8
    # sed -i "s|drrs.cache.capacity=[0-9]\{1,\}|drrs.cache.capacity=200|g" $SCALE_CONF

    echo "study 2: only subscale division"
    sed -i "s|drrs.cache.capacity=[0-9]\{1,\}|drrs.cache.capacity=1|g" $SCALE_CONF
    sed -i "s|drrs.enable-dr=[a-z]\{1,\}|drrs.enable-dr=false|g" $SCALE_CONF
    $0 evaluate drrs $workload
    sed -i "s|drrs.cache.capacity=[0-9]\{1,\}|drrs.cache.capacity=200|g" $SCALE_CONF
    sed -i "s|drrs.enable-dr=[a-z]\{1,\}|drrs.enable-dr=true|g" $SCALE_CONF


    # study 3: no decouping and rerouting
    # drrs.enable-dr
    # echo "study 3: no decouping and rerouting"
    # sed -i "s|drrs.enable-dr=[a-z]\{1,\}|drrs.enable-dr=false|g" $SCALE_CONF
    # $0 evaluate drrs q8
    # sed -i "s|drrs.enable-dr=[a-z]\{1,\}|drrs.enable-dr=true|g" $SCALE_CONF

    echo "study 3: only reordering"
    sed -i "s|drrs.enable-dr=[a-z]\{1,\}|drrs.enable-dr=false|g" $SCALE_CONF
    sed -i "s|\"dull-scheduler\": [a-z]\{1,\}|\"dull-scheduler\": true|g" $CONTROLLER_CONFIG
    sed -i "s|drrs.enable-subscale=[a-z]\{1,\}|drrs.enable-subscale=false|g" $SCALE_CONF
    $0 evaluate drrs $workload
    sed -i "s|drrs.enable-dr=[a-z]\{1,\}|drrs.enable-dr=true|g" $SCALE_CONF
    sed -i "s|\"dull-scheduler\": [a-z]\{1,\}|\"dull-scheduler\": false|g" $CONTROLLER_CONFIG
    sed -i "s|drrs.enable-subscale=[a-z]\{1,\}|drrs.enable-subscale=true|g" $SCALE_CONF

    exit 0
elif [ $1 = "test" ]; then
    ################################# test 1: single node #################################
    default_cache_capacity=200
    drrs_set_migration_buffer_capacity $default_cache_capacity
    drrs_set_inter_subscale_scheduler "min_hold"
    drrs_set_intra_subscale_scheduler "lex"

    $0 stop
    default_running_nums=5
    for i in $(seq 1 $default_running_nums)
    do
        for default_workload in "q8" "q7" "twitch" 
        do
            for approach in "otfs"
            do
                # echo "Approach $approach with workload $default_workload"
                $0 evaluate $approach $default_workload
                # if not ok, then exit
                if [ $? -ne 0 ]; then
                    echo "Approach $approach with workload $default_workload is not ok!"
                    exit 1
                fi
            done
        done
    done

    # exit 0
    ################################# test 2: sensitivity analysis #################################

    # 1250000 = 1.25MB * 4096 = 5GB(total)
    # 2500000 = 2.5MB * 4096 = 10GB(total)
    # 3750000 = 3.75MB * 4096 = 15GB(total)
    # 5000000 = 5MB * 4096 = 20GB(total)
    # 7500000 = 7.5MB * 4096 = 30GB(total)
    # $0 stop

    # state_size(per-key): 250000, 1250000, 2500000, 5000000, 7500000, 10000000
    # input_rate(total): 1000, 2000, 4000, 6000
    # skewness: 0, 0.5, 0.75, 1, 1.25, 1.5, 2
    # for state_size in 5000000 
    # do
    #     for input_rate in 8000
    #     do
    #         for skewness in 0.0
    #         do
    #             set_sensitive_analysis_config $state_size $input_rate $skewness
    #             for i in $(seq 1 $default_running_nums)
    #             do
    #                 for approach in megaphone
    #                 do
    #                     $0 swarm $approach fwc
    #                     # if not ok, then exit
    #                     if [ $? -ne 0 ]; then
    #                         echo "Approach $approach with workload fwc is not work"
    #                         exit 1
    #                     fi
    #                 done
    #             done
    #         done
    #     done
    # done

    # meces-1.25MB-15000-0.0

    # 3750000 10000
    # for skewness in 0.0 0.5
    # do  
    #     for approach in megaphone meces
    #     do
    #         set_sensitive_analysis_config 2500000 15000 $skewness
    #         $0 swarm $approach fwc
    #     done
    # done

    exit 0
elif [ $1 = "stop" ]; then

    # download docker logs to local
    if [ ! -d "$LOG_DIR/tmp" ]; then
        mkdir -p $LOG_DIR/tmp
    fi
    containers=$(docker ps --filter "name=docker-taskmanager" --format "{{.Names}}")
    for container in $containers
        do
        log_file="${LOG_DIR}/tmp/${container}.log"
        # echo "Downloading logs for $container to $log_file"
        docker logs "$container" > "$log_file" 2>&1
        done
    
    
    docker logs "docker-jobmanager-1" > "${LOG_DIR}/tmp/jobmanager.log" 2>&1

    stop_clusters

    # rename tmp folder to: manual-stop-timestamp(day-hour-minute)
    if [ -d "$LOG_DIR/tmp" ]; then
        timestamp=$(date +"%d-%H-%M")
        mv $LOG_DIR/tmp $LOG_DIR/manual-stop-$timestamp
    fi
    exit 0
else
    echo "Wrong argument!"
    exit 1
fi
