#!/bin/bash

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
  --expname)
    expname="$2"
    shift # past argument
    shift # past value
    ;;
  --nruns)
    nruns="$2"
    shift # past argument
    shift # past value
    ;;
  --nnodes)
    nnodes="$2"
    shift # past argument
    shift # past value
    ;;
  --ninitnodes)
    ninitnodes="$2"
    shift # past argument
    shift # past value
    ;;
  --protocols)
    protocols="$2"
    shift # past argument
    shift # past value
    ;;
  --probs)
    probs="$2"
    shift # past argument
    shift # past value
    ;;
  *) # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift              # past argument
    ;;
  esac
done

set -- "${POSITIONAL[@]}" # restore positional parameters

if [[ -z "${expname}" ]]; then
  echo "expname not set"
  exit
fi
if [[ -z "${nruns}" ]]; then
  echo "nruns not set"
  exit
fi
if [[ -z "${nnodes}" ]]; then
  echo "nnodes not set"
  exit
fi
if [[ -z "${ninitnodes}" ]]; then
  echo "ninitnodes not set"
  exit
fi
if [[ -z "${protocols}" ]]; then
  echo "protocols not set"
  exit
fi
if [[ -z "${probs}" ]]; then
  echo "probs not set"
  exit
fi

IFS=', ' read -r -a expName <<<"$expname"
IFS=', ' read -r -a nRunsList <<<"$nruns"
IFS=', ' read -r -a nNodesList <<<"$nnodes"
IFS=', ' read -r -a nInitNodesList <<<"$ninitnodes"
IFS=', ' read -r -a protocolList <<<"$protocols"
IFS=', ' read -r -a probabilityList <<<"$probs"

# ./runExperiments --nnodes 50,100,150,200 --ninitnodes 50,100,150,200 --protocols plumtree,flood --probability 1,0.5,0.3 --nruns 2,3
echo "Killing previous existing containers"
for n in $(oarprint host); do
  oarsh -n $n 'docker kill $(docker ps -aq)';
done
sleep 15
docker network remove crdtsnet
sleep 5
docker network create -d overlay --attachable --subnet 10.10.0.0/24 --gateway 10.10.0.1 crdtsnet
sleep 10

pos=0
for nNodes in "${nNodesList[@]}"; do
  echo Starting nNodes $nNodes
  bash docker/distributeContainers.sh "latencies${nNodes}.txt"
  sleep 30
  warmup=$nNodes
  runtime=300
  echo Warmup is $warmup
  echo Runtime is $runtime
  for protocol in "${protocolList[@]}"; do
    echo Starting protocol $protocol
    if [[ "$protocol" == "flood" ]]; then
      cooldown=600
    else
      cooldown=300
    fi
    echo Cooldown is $cooldown

    for probability in "${probabilityList[@]}"; do
      echo Starting probability $probability
      for run in "${nRunsList[@]}"; do
        echo Starting run $run
        exp_path="/logs/${nNodes}nodes/${protocol}/prob${probability}/${run}runs"
        echo Exp_path is $exp_path

        for node in $(oarprint host); do
          oarsh $node "mkdir -p /tmp${exp_path}"
        done

        docker exec -d node_0 ./start.sh $protocol $probability $warmup $runtime $cooldown $exp_path
        sleep 0.5

        contactnode="node_0:5000"

        mapfile -t hosts < <(uniq $OAR_FILE_NODES)
        serverNodes=$(uniq $OAR_FILE_NODES | wc -l)
        perHost=$((nNodes / serverNodes))

        inode=$((nInitNodesList[pos]))
        echo inode is $inode
        for ((nodeNumber=1;nodeNumber<inode;nodeNumber++)); do
          node=$((nodeNumber/perHost))
          echo node $nodeNumber host ${hosts[node]}
          oarsh -n ${hosts[node]} "docker exec -d node_${nodeNumber} ./start.sh $protocol $probability $warmup $runtime $cooldown $exp_path ${contactnode}:5000"
          sleep 0.5
        done

        sleep 230

        newRuntime=$((runtime + warmup - 2 - 230))
        echo Newruntime is $newRuntime
        for ((nextNodeNumber=nodeNumber;nextNodeNumber<nNodes;nextNodeNumber++)); do
          node=$((nextNodeNumber/perHost))
          echo node $nextNodeNumber host ${hosts[node]}
          oarsh -n ${hosts[node]} "docker exec -d node_${nextNodeNumber} ./start.sh $protocol $probability 2 $newRuntime $cooldown $exp_path ${contactnode}:5000"
          sleep 0.5
        done

        sleep_time=$((warmup + cooldown + 300 + 10 + 20 - 230))
        echo Sleeping $sleep_time $(date)
        sleep $sleep_time
      done #run
    done #probability
  done #protocol
  echo "Killing all containers"
  for n in $(oarprint host); do
    oarsh -n $n 'docker kill $(docker ps -aq)'
  done
  sleep 15
  pos=pos+1
done #nNodes
for n in $(oarprint host); do
    oarsh -n $n "$HOME/PlumtreeOpLogs/docker/compressLogs.sh $expName $n"
  done
exit
