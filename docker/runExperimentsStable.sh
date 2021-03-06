#!/bin/bash

### USAGE ###
# ./runExperimentsStable.sh --expname test1 --nnodes 50,100,150,200 --protocols plumtree,flood --payloads 128,256,512,1024 --probs 1,0.5,0.3 --nruns 1,2,3 --runtime 900

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
if [[ -z "${protocols}" ]]; then
  echo "protocols not set"
  exit
fi
if [[ -z "${probs}" ]]; then
  probs="1"
fi
if [[ -z "${payloads}" ]]; then
  payloads="128"
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

### START OF EXPERIMENTS ###
for protocol in "${protocolList[@]}"; do
  echo Starting protocol $protocol
  for payload in "${payloadList[@]}"; do
    echo Starting payload $payload
    for probability in "${probabilityList[@]}"; do
      echo Starting probability $probability
      for run in "${runsList[@]}"; do
        echo Starting run $run
        exp_path="/logs/${expname}/${nnodes}nodes/${protocol}/payload${payload}/prob${probability}/${run}runs"
        output="/tmp/logs/${expname}/${nnodes}nodes/${protocol}/payload${payload}/prob${probability}/${run}runs/output.txt"

        ### REMOVING THE OPERATION FILES ###
        for node in $(oarprint host); do
          oarsh $node "sudo-g5k rm /tmp/data/*"
          oarsh $node "sudo-g5k mkdir -p /tmp${exp_path}"
        done
        sudo-g5k touch ${output}

        mapfile -t hosts < <(uniq $OAR_FILE_NODES)
        serverNodes=$(uniq $OAR_FILE_NODES | wc -l)
        perHost=$((nnodes / serverNodes))
        if [[ "$perHost" -ne 50 ]]; then
          echo "perHost is $perHost (which is not 50)"
        fi

        ### LAUNCHING NODES ###
        port=5000
        turn=0
        echo node 0 host ${hosts[0]}
        docker exec -d node_0 ./start.sh $protocol $probability $payload $warmup $runtime $cooldown $exp_path $port $turn
        startDate=$(date -u)
        echo "FIRST_NODE $startDate" | sudo-g5k tee $output
        firstDeadTime=$(date -u -d "$startDate +$((warmup + runtime + cooldown)) seconds")
        firstCooldownTime=$(date -u -d "$startDate +$((warmup + runtime)) seconds")
        firstMessage=$(date -u -d "$startDate +$((warmup)) seconds")
        echo FIRST_DEAD $firstDeadTime | sudo-g5k tee -a $output
        echo FIRST_COOLDOWN $firstCooldownTime | sudo-g5k tee -a $output
        echo FIRST_MESSAGE $firstMessage | sudo-g5k tee -a $output
        sleep 0.5
        contactnode="node_0:5000"

        for ((nodeNumber = 1; nodeNumber < nnodes; nodeNumber++)); do
          node=$((nodeNumber/perHost))
          echo node $nodeNumber host ${hosts[node]}
          oarsh -n ${hosts[node]} "docker exec -d node_${nodeNumber} ./start.sh $protocol $probability $payload $warmup $runtime $cooldown $exp_path $port $turn ${contactnode}"
          if [[ $nodeNumber -eq $((nnodes - 1)) ]]; then
            echo "LAST_NODE $(date -u)" | sudo-g5k tee -a $output
          fi
          sleep 0.5
        done

        ### WAITING UNTIL END ###
        exit_time=20
        sleep_time=$((warmup + runtime + cooldown + exit_time))
        echo Sleeping $sleep_time seconds
        finalTime=$(date -u -d "+${sleep_time} seconds")
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
sudo-g5k chown -R evieira:g5k-users /tmp/logs
nodes=$(uniq "$OAR_NODEFILE" | sed -n '1!p')
for n in $nodes; do
  oarsh -n $n "sudo-g5k chown -R evieira:g5k-users /tmp/logs"
  rsync -e 'oarsh' -arzP $n:/tmp/logs/* /tmp/logs &
done
wait
tar -czvf ${HOME}/${expname}_${nnodes}.tar.gz /tmp/logs

### DELETING LOGS ###
for n in $(oarprint host); do
    oarsh -n $n "sudo-g5k rm -rf /tmp/logs" &
done
wait

exit