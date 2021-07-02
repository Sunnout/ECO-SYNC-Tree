#!/bin/bash

function help {
    echo "usage: $0 <setupScript> <dockerImage> <path/to/configFile> [bandwidth]"
}

setupScript=$(pwd)/docker/launchContainers.sh
image="crdts-plumtree:exp.0.1"
config=$(pwd)/docker/ips100.txt

bandwidth=$4

if [ -z $bandwidth ]; then
  bandwidth=10000
fi

net=$DOCKER_NET
vol=$DOCKER_VOL

n_nodes=$(uniq $OAR_FILE_NODES | wc -l)

if [ -z $net ]; then
  echo "Docker net is not setup, pls run setup first"
  help
  exit
fi

if [ -z $image ]; then
  echo "Pls specify a Docker image"
  help
  exit
fi

if [ -z $config ]; then
  echo "Pls specify config file"
  help
  exit
fi

nContainers=$(cat $config | wc -l)
perHost=$((nContainers / n_nodes))
i=0
for n in $(uniq $OAR_FILE_NODES); do
  off=$((i*perHost))
  if [ $((i+1)) -eq $n_nodes ]; then
    mark=$nContainers
  else
    mark=$(((i+1)*perHost))
  fi

  oarsh -n $n "$setupScript $off $mark $config $image $bandwidth $net $vol"
  i=$((i+1))
done

wait
