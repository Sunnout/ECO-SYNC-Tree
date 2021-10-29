#!/bin/sh

protocol=$1
probability=$2
payload=$3
warmup=$4
runtime=$5
cooldown=$6
exppath=$7
contactnode=$8

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
  java -Xmx1024M -DlogFilename=${exppath}/${servernode} -jar PlumtreeOpLogs.jar -conf config.properties interface=eth0 bcast_protocol=${protocol} op_probability=${probability} payload_size=${payload} create_time=${warmup} run_time=${runtime} cooldown_time=${cooldown}
else
  java -Xmx1024M -DlogFilename=${exppath}/${servernode} -jar PlumtreeOpLogs.jar -conf config.properties interface=eth0 bcast_protocol=${protocol} op_probability=${probability} payload_size=${payload} create_time=${warmup} run_time=${runtime} cooldown_time=${cooldown} contact=${contactnode}
fi
