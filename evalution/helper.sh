#!/bin/bash
DOCKER_COMPOSE_FILE="/path/to/docker/docker-compose.yaml"
DOCKER_COMPOSE_SWARM_FILE="/path/to/docker/docker-compose.swarm.yml"

STANDALONE_EXTERNAL_NETWORK="custom_net"
SWARM_EXTERNAL_NETWORK="flink_swarm_net"
SWARM_STACK_NAME="evaluation"

CHECKFILE1="/path/to/docker/entrypoint.sh"
CHECKFILE2="/path/to/docker/Dockerfile.datagen"
CHECKFILE3="/path/to/docker/Dockerfile"

# check the fisrt argument should identify the action on swarm or standalone
# if the first argument is not "swarm" or "standalone", then exit
if [ "$1" != "swarm" ] && [ "$1" != "standalone" ]; then
    echo "Please provide the first argument as 'swarm' or 'standalone'"
    exit 1
fi

if [ "$2" = "stop" ]; then
    if [ "$1" = "swarm" ]; then
        docker stack rm $SWARM_STACK_NAME
    else
        docker compose -f $DOCKER_COMPOSE_FILE down >/dev/null 2>&1
    fi
    return 0
fi

# assert there are 2 arguments
if [ "$#" -ne 3 ]; then
    echo "Please provide 2 arguments:  $0 [swarm|standalone] [system_type] [replica_number(for taskmanager)]"
    exit 1
fi

if [ "$1" = "standalone" ]; then
    # check if the STANDALONE_EXTERNAL_NETWORK exists
    if [ -z "$(docker network ls --filter name=^${STANDALONE_EXTERNAL_NETWORK}$ --format='{{.Name}}')" ]; then    
        echo "Creating external network..."
        docker network create $STANDALONE_EXTERNAL_NETWORK
        if [ $? -ne 0 ]; then
            echo "Failed to create external network"
            exit 1
        fi
        echo "External network $STANDALONE_EXTERNAL_NETWORK created"
    fi
else
    # check if the SWARM_EXTERNAL_NETWORK exists
    if [ -z "$(docker network ls --filter name=${SWARM_EXTERNAL_NETWORK} --format='{{.Name}}')" ]; then
        echo "Creating external network..."
        docker network create -d overlay $SWARM_EXTERNAL_NETWORK
        if [ $? -ne 0 ]; then
            echo "Failed to create external network"
            exit 1
        fi
        echo "External network $SWARM_EXTERNAL_NETWORK created"
    fi
fi



if [ "$1" = "swarm" ]; then
    echo "Updating docker-compose.swarm.yaml for $2 cluster..."
    sed -i "s|SYSTEM_TYPE=[-0-9a-zA-Z_]\+|SYSTEM_TYPE=$2|g" $DOCKER_COMPOSE_SWARM_FILE
    sed -i "/taskmanager:/,/replicas:/s/replicas: [0-9]*/replicas: $3/" $DOCKER_COMPOSE_SWARM_FILE
    docker stack deploy -c $DOCKER_COMPOSE_SWARM_FILE $SWARM_STACK_NAME
else
    echo "Updating docker-compose.yaml for $2 cluster with $3 taskmanagers..."
    sed -i "s|SYSTEM_TYPE=[-0-9a-zA-Z_]\+|SYSTEM_TYPE=$2|g" $DOCKER_COMPOSE_FILE
    sed -i "s/replicas: [0-9]*/replicas: $3/g" $DOCKER_COMPOSE_FILE
    # no output to stdout
    docker compose -f  $DOCKER_COMPOSE_FILE up -d >/dev/null 2>&1
fi

