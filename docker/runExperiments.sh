#!/bin/sh

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
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

if [[ -z "${nruns}" ]]; then
  echo "nruns not set"
  exit
fi
if [[ -z "${nnodes}" ]]; then
  echo "nnodes not set"
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

IFS=', ' read -r -a nRunsList <<<"$nnruns"
IFS=', ' read -r -a nNodesList <<<"$nnodes"
IFS=', ' read -r -a protocolList <<<"$protocols"
IFS=', ' read -r -a probabilityList <<<"$probs"

# ./runExperiments --nnodes 50,100,150,200 --protocols plumtree,flood --probability 1,0.5,0.3 --nruns 2,3

for nNodes in "${nNodesList[@]}"; do
  echo Starting nNodes $nNodes
  bash distributeContainers.sh "latencies${nNodes}.txt"
  sleep 120
  # lancar os containers
  # esperar X tempo pelo setupTC
  warmup=$((nNodes/2 + 30))
  echo Warmup is $warmup
  for protocol in "${protocolList[@]}"; do
    echo Starting protocol $protocol
    if [[ $protocol -eq "flood"]]
      cooldown=600
    elif
      cooldown=300
    fi
    echo Cooldown is $cooldown

    for $probability in "${probabilityList[@]}"; do
      echo Starting probability $probability
      for run in "${nRunsList[@]}"; do
        echo Starting run $run
        exp_path="/logs/${nNodes}/${protocol}/${probability}/${run}"
        echo Exp_path is $exp_path

        for node in $(oarprint host); do
          oarsh $node "mkdir -p /tmp${exp_path}"
        done

        docker exec -d node_0 ./start.sh $protocol $probability $warmup $cooldown $exp_path
        sleep 0.5

        contactnode="node_0:5000"
        for nodeNumber in $(seq 1 nNodes); do
          docker exec -d node_${nodeNumber} ./start.sh $protocol $probability $warmup $cooldown $exp_path ${contactnode}:5000
          sleep 0.5
        done

      done #run
    done #probability
  done #protocol
  echo "Killing all containers"
  for n in $(oarprint host); do
    oarsh -n $n 'docker kill $(docker ps -aq)';
  done
done #nNodes
exit
