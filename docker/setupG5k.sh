#!/bin/bash

subnet="10.10.0.0/24"
gateway="10.10.0.1"
name="crdtsnet"
volume="/tmp/logs"
docker network create -d overlay --attachable --subnet $subnet --gateway $gateway $name

if [ -z $subnet ] || [ -z $gateway ] || [ -z $name ] || [ -z $volume ]; then
  echo "setup needs exactly 3 arguments"
  echo "setup.sh <subnet> <gateway> <net_name> <volume_name>"
  exit
fi

for node in $(oarprint host); do
  oarsh $node "sudo-g5k /grid5000/code/bin/g5k-setup-docker" &
done
wait

docker swarm init
JOIN_TOKEN=$(docker swarm join-token manager -q)

host=$(hostname)
for node in $(oarprint host); do
  if [ $node != $host ]; then
    oarsh $node "docker swarm join --token $JOIN_TOKEN $host:2377"
  fi
done

for node in $(oarprint host); do
  oarsh $node "mkdir $volume" &
done
wait

docker network create -d overlay --attachable --subnet $subnet --gateway $gateway $name

export DOCKER_NET="$name"
export DOCKER_VOL="$volume"
