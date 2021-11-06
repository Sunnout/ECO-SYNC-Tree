#!/bin/bash

### USAGE ###
# ./runExperimentsStable.sh --expname test1 --nnodes 50,100,150,200 --interval 2 --ndeadandnewnodes 1
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
  --ndeadandnewnodes)
    ndeadandnewnodes="$2"
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
if [[ -z "${ndeadandnewnodes}" ]]; then
  echo "ndeadandnewnodes not set"
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

        for node in $(oarprint host); do
          oarsh $node "sudo-g5k mkdir -p /tmp${exp_path}"
        done
        sudo-g5k touch ${output}

        mapfile -t hosts < <(uniq $OAR_FILE_NODES)
        serverNodes=$(uniq $OAR_FILE_NODES | wc -l)
        perHost=$((nnodes / serverNodes))
        if [[ "$perHost" -ne 50 ]]; then
          echo "perHost is $perHost (which is not 50)"
        fi

        ### LAUNCHING INITIAL NODES ###
        port=5000
        turn=0
        echo Starting $nnodes initial nodes

        echo node 0 port $port host ${hosts[0]}
        docker exec -d node_0 ./start.sh $protocol $probability $payload $warmup $runtime $cooldown $exp_path $port $turn
        echo "FIRST_NODE $(date -u)" | sudo-g5k tee $output
        firstDeadTime=$(date -u -d "+$((warmup + runtime + cooldown)) seconds")
        echo FIRST_DEAD $firstDeadTime | sudo-g5k tee -a $output
        sleep 0.5
        contactnode="node_0:5000"

        for ((nodeNumber = 1; nodeNumber < nnodes; nodeNumber++)); do
          node=$((nodeNumber/perHost))
          echo node $nodeNumber port $port host ${hosts[node]}
          oarsh -n ${hosts[node]} "docker exec -d node_${nodeNumber} ./start.sh $protocol $probability $payload $warmup $runtime $cooldown $exp_path $port $turn ${contactnode}"
          if [[ $nodeNumber -eq $((nnodes - 1)) ]]; then
            echo "LAST_NODE $(date -u)" | sudo-g5k tee -a $output
          fi
          sleep 0.5
        done

        ### WAITING UNTIL LAST NODE WARMS UP ###
        echo Sleeping $warmup seconds
        sleep $warmup

        ### CHURN STEP ###
        nChanges=$((runtime/interval - 1))
        startTime=$(date +%s)
        echo "START_CHURN $(date -u)" | sudo-g5k tee -a $output
        for ((change = 0; change < nChanges; change++)); do
          echo Change number $((change+1))

          ### KILLING NODES AND REVIVING THEM WITH DIFFERENT PORT ###
          for ((deadAndNew = 0; deadAndNew < ndeadandnewnodes; deadAndNew++)); do
            if [[ $nodeNumber -eq $nnodes ]]; then
              nodeNumber=1
              port=$((port + 2000))
              turn=$((turn + 1))
            fi
            node=$((nodeNumber/perHost))

            ### KILLING NODES ###
            echo Killing node_$nodeNumber port $((port - 2000))
            oarsh -n ${hosts[node]} "docker exec -d node_${nodeNumber} killall java"

            ### REVIVING NODES WITH DIFFERENT PORT AND LOG FILE ###
            newWarmup=5
            timePassed=$(($(date +%s) - startTime))
            newRuntime=$((runtime - timePassed - newWarmup))
            echo New runtime is $newRuntime
            echo node $nodeNumber port $port host ${hosts[node]}
            oarsh -n ${hosts[node]} "docker exec -d node_${nodeNumber} ./start.sh $protocol $probability $payload $newWarmup $newRuntime $cooldown $exp_path $port $turn ${contactnode}"

            nodeNumber=$((nodeNumber + 1))
          done #killing and reviving nodes

          sleepUntil=$((startTime + (change + 1) * interval))
          sleepTime=$((sleepUntil - $(date +%s)))
          echo Sleeping $sleepTime seconds before next change
          sleep $sleepTime
        done #change
        echo "END_CHURN $(date -u -d "-${sleep_time} seconds")" | sudo-g5k tee -a $output

        ### WAITING UNTIL END ###
        exit_time=20
        sleep_time=$((startTime + runtime + cooldown - $(date +%s) + exit_time))
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
tar -czvf $HOME/$expname$nnodes.tar.gz /tmp/logs

### DELETING LOGS ###
for n in $(oarprint host); do
    oarsh -n $n "sudo-g5k rm -rf /tmp/logs" &
done
wait

exit