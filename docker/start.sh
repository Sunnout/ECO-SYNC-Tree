#!/bin/sh

run=$1
protocol=$2
nnodes=$3
probability=$4
warmup=$5
cooldown=$6
exppath=$7
contactnode=$8

servernode=$(hostname)

if [ -z $run ]; then
  echo "Pls specify number of runs"
  exit
fi

if [ -z $protocol ]; then
  echo "Pls specify a broadcast protocol"
  exit
fi

if [ -z $nnodes ]; then
  echo "Pls specify number of nodes"
  exit
fi

if [ -z $probability ]; then
  echo "Pls specify a operation probability"
  exit
fi

if [ -z $warmup ]; then
  echo "Pls specify warmup time"
  exit
fi

if [ -z $cooldown ]; then
  echo "Pls specify cooldown time"
  exit
fi

if [ -z $contactnode ]; then
  echo "Pls specify a broadcast protocol"
  exit
fi

if [ -z $exppath ]; then
  echo "Pls specify exppath"
  exit
fi

#mkdir logs/$exppath

#java -DlogFilename=${exppath}_${nnodes}_${dissemination}_${servernode}.log -cp ./net.jar Main -overlay $overlay -dissemination $dissemination -babelConfFile ./network_config.properties $4 2> logs/${exppath}/${overlay}_${dissemination}_${servernode}.err

java -Xmx1024M -DlogFilename=${exppath}/${servernode}.log -jar PlumtreeOpLogs.jar -conf config.properties address=$servernode contact=$contactnode bcast_protocol=$protocol op_probability=$probability create_time=$warmup cooldown_time=$cooldown
