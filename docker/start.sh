#!/bin/sh

protocol=$1
probability=$2
warmup=$3
cooldown=$4
exppath=$5
contactnode=$6

servernode=$(hostname)

if [ -z $protocol ]; then
  echo "Pls specify a broadcast protocol"
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

if [ -z $exppath ]; then
  echo "Pls specify exppath"
  exit
fi

#mkdir logs/$exppath

#java -DlogFilename=${exppath}_${nnodes}_${dissemination}_${servernode}.log -cp ./net.jar Main -overlay $overlay -dissemination $dissemination -babelConfFile ./network_config.properties $4 2> logs/${exppath}/${overlay}_${dissemination}_${servernode}.err

if [ -z $contactnode ]; then
  java -Xmx1024M -DlogFilename=${exppath}/${servernode} -jar PlumtreeOpLogs.jar -conf config.properties interface=eth0 bcast_protocol=${protocol} op_probability=${probability} create_time=${warmup} cooldown_time=${cooldown}
else
  java -Xmx1024M -DlogFilename=${exppath}/${servernode} -jar PlumtreeOpLogs.jar -conf config.properties interface=eth0 bcast_protocol=${protocol} op_probability=${probability} create_time=${warmup} cooldown_time=${cooldown} contact=${contactnode}
fi
