#!/bin/sh

protocol=$1
probability=$2
warmup=$3
runtime=$4
cooldown=$5
exppath=$6
contactnode=$7

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

if [ -z $runtime ]; then
  echo "Pls specify run time"
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

if [ -z $contactnode ]; then
  java -Xmx1024M -DlogFilename=${exppath}/${servernode} -jar PlumtreeOpLogs.jar -conf config.properties interface=eth0 bcast_protocol=${protocol} op_probability=${probability} create_time=${warmup} run_time=${runtime} cooldown_time=${cooldown}
else
  java -Xmx1024M -DlogFilename=${exppath}/${servernode} -jar PlumtreeOpLogs.jar -conf config.properties interface=eth0 bcast_protocol=${protocol} op_probability=${probability} create_time=${warmup} run_time=${runtime} cooldown_time=${cooldown} contact=${contactnode}
fi
