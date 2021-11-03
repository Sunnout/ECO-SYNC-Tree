#!/bin/bash

### USAGE ###
# ./runExperimentsStable.sh --expname test1 --nnodes 50,100,150,200 --interval 2 --nnewnodes 1 --ndeadnodes 1
# --protocols plumtree,flood --payloads 128,256,512,1024 --probs 1,0.5,0.3 --nruns 1,2,3 --runtime 900

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
  --expname)
    expname="$2"
    shift # past argument
    shift # past value
    ;;
  --runtime)
    runtime="$2"
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
  --nnewnodes)
    nnewnodes="$2"
    shift # past argument
    shift # past value
    ;;
  --ndeadnodes)
    ndeadnodes="$2"
    shift # past argument
    shift # past value
    ;;
  --interval)
    interval="$2"
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
  --payloads)
    payloads="$2"
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
if [[ -z "${runtime}" ]]; then
  runtime="300"
fi
if [[ -z "${nruns}" ]]; then
  echo "nruns not set"
  exit
fi
if [[ -z "${nnodes}" ]]; then
  echo "nnodes not set"
  exit
fi
if [[ -z "${nnewnodes}" ]]; then
  echo "nnewnodes not set"
  exit
fi
if [[ -z "${ndeadnodes}" ]]; then
  echo "ndeadnodes not set"
  exit
fi
if [[ -z "${protocols}" ]]; then
  echo "protocols not set"
  exit
fi
if [[ -z "${payloads}" ]]; then
  payloads="128"
fi
if [[ -z "${probs}" ]]; then
  probs="1"
fi
if [[ -z "${interval}" ]]; then
  echo "interval not set"
  exit
fi

IFS=', ' read -r -a runsList <<<"$nruns"
IFS=', ' read -r -a protocolList <<<"$protocols"
IFS=', ' read -r -a probabilityList <<<"$probs"
IFS=', ' read -r -a payloadList <<<"$payloads"

echo "Killing previous existing containers"
for n in $(oarprint host); do
  oarsh -n $n 'docker kill $(docker ps -aq)';
done
sleep 15
docker network remove crdtsnet
sleep 5
docker network create -d overlay --attachable --subnet 10.10.0.0/24 --gateway 10.10.0.1 crdtsnet
sleep 10

echo Starting $nnodes nodes
bash docker/distributeContainers.sh "latencies${nnodes}.txt"
sleep 30

warmup=$nnodes
cooldown=$((nnodes + 120))
echo Warmup is $warmup
echo Runtime is $runtime
echo Cooldown is $cooldown

nChanges=$((runtime/interval))

### START OF EXPERIMENTS ###
for protocol in "${protocolList[@]}"; do
  echo Starting protocol $protocol
  for payload in "${payloadList[@]}"; do
    echo Starting payload $payload
    for probability in "${probabilityList[@]}"; do
      echo Starting probability $probability
      for run in "${runsList[@]}"; do
        echo Starting run $run
        exp_path="/logs/${nnodes}nodes/${protocol}/payload${payload}/prob${probability}/${run}runs"

        for node in $(oarprint host); do
          oarsh $node "mkdir -p /tmp${exp_path}"
        done

        mapfile -t hosts < <(uniq $OAR_FILE_NODES)
        serverNodes=$(uniq $OAR_FILE_NODES | wc -l)
        perHost=$((nnodes / serverNodes))
        if [[ "$perHost" -ne 50 ]]; then
          echo "perHost is $perHost (which is not 50)"
        fi

        ### LAUNCHING INITIAL NODES ###
        echo Starting $nnodes initial nodes

        echo node 0 host ${hosts[0]}
        docker exec -d node_0 ./start.sh $protocol $probability $payload $warmup $runtime $cooldown $exp_path
        sleep 0.5
        contactnode="node_0:5000"

        for ((nodeNumber = 1; nodeNumber < nnodes; nodeNumber++)); do
          node=$((nodeNumber/perHost))
          echo node $nodeNumber host ${hosts[node]}
          oarsh -n ${hosts[node]} "docker exec -d node_${nodeNumber} ./start.sh $protocol $probability $payload $warmup $runtime $cooldown $exp_path ${contactnode}"
          sleep 0.5
        done

        ### WAITING UNTIL LAST NODE WARMS UP ###
        timeToChurn=$((warmup))
        echo Sleeping $timeToChurn seconds
        sleep $timeToChurn

        ### CHURN STEP ###
        toKill=1
        timePassed=0
        for ((change = 0; change <= nChanges; change++)); do
          echo Change number $((change+1))

          ### KILLING NODES ###
          for ((dead = 0; dead < ndeadnodes; dead++)); do
            node=$((toKill/perHost))
            echo Killing node_$toKill
            oarsh -n ${hosts[node]} "docker exec -d node_${toKill} killall java"
            toKill=$((toKill+1))
          done

          ### LAUNCHING NEW NODES ###
          toStart=$nnodes
          newWarmup=5
          echo New runtime is $newRuntime
          for ((new = 0; new < nnewnodes; new++)); do
            newRuntime=$((runtime - timeToChurn - timePassed + warmup - newWarmup))
            node=$((toStart/perHost))
            echo node $toStart host ${hosts[node]}
            oarsh -n ${hosts[node]} "docker exec -d node_${toStart} ./start.sh $protocol $probability $payload $newWarmup $newRuntime $cooldown $exp_path ${contactnode}"
            toStart=$((toStart+1))
            timePassed=$((timePassed + 0.5))
            sleep 0.5
          done

          timePassed=$((timePassed + interval))
          echo Sleeping $interval before next change
          sleep $interval
        done #change

        ### WAITING UNTIL END ###
        sleep_time=$((warmup + runtime + cooldown - timeToChurn - timePassed))
        echo Sleeping $sleep_time seconds
        finalTime=$(date -d "+${sleep_time} seconds")
        echo Run $run ends at $finalTime
        sleep $sleep_time
      done #run
    done #probability
  done #payload
done #protocol
echo "Killing all containers"
for n in $(oarprint host); do
  oarsh -n $n 'docker kill $(docker ps -aq)'
done
sleep 15

### COMPRESSING LOGS ###
for n in $(oarprint host); do
    oarsh -n $n "$HOME/PlumtreeOpLogs/docker/compressLogs.sh $expname $n" &
done
wait

exit
