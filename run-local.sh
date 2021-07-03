#!/bin/bash

processes=$1
newprocesses=$2
killprocesses=$3
contactnode=$4
runcontact=$5

if [ -z $processes ] || [ $processes -lt 1 ]; then
  echo "please indicate a number of processes of at least one"
  exit 0
fi

if [ -z $newprocesses ] || [ $newprocesses -lt 0 ]; then
  echo "please indicate a number of new processes. If none enter 0"
  exit 0
fi

if [ -z $killprocesses ] || [ $killprocesses -lt 0 ] || [ $killprocesses -gt $processes ]; then
  echo "please indicate a number of processes to kill (must be less or equal to initial processes). If none enter 0"
  exit 0
fi

if [ -z $contactnode ]; then
  echo "please indicate ip:port of contact node"
  exit 0
fi

i=0
k=0
port=5000
bcastport=6000

if [ ! -z $runcontact ]; then
  java -Xmx1024M -DlogFilename=/tmp/plumtreelogs/results-$(hostname)-$[$port+$i] -jar target/PlumtreeOpLogs.jar -conf config.properties address=$(hostname) port=$port bcast_port=$bcastport | sed "s/^/[$(($port + $i))] /"&
  echo "launched contact on port $port"
  i=1
fi

sleep 0.5

while [ $i -lt $processes ]
do
	java -Xmx1024M -DlogFilename=/tmp/plumtreelogs/results-$(hostname)-$[$port+$i] -jar target/PlumtreeOpLogs.jar -conf config.properties address=$(hostname) port=$[$port+$i] bcast_port=$[$bcastport+$i] contact=$contactnode | sed "s/^/[$(($port + $i))] /"&
	echo "launched process on port $[$port+$i]"
	i=$[$i+1]
	sleep 0.5
done

sleep 25
k=1 #Start from 1 so as not to kill contact node

while [ $k -le $killprocesses ]
do
  kill $(ps aux | grep "port=$[$port+$k]" | awk '{print $2}')
  k=$[$k+1]
  sleep 1
done

sleep 5
j=0

while [ $j -lt $newprocesses ]
do
  java -Xmx1024M -DlogFilename=/tmp/plumtreelogs/results-$(hostname)-$[$port+$i] -jar target/PlumtreeOpLogs.jar -conf config-newnode.properties address=$(hostname) port=$[$port+$i] bcast_port=$[$bcastport+$i] contact=$contactnode | sed "s/^/[$(($port + $i))] /"&
  echo "launched process on port $[$port+$i]"
  i=$[$i+1]
  j=$[$j+1]
	sleep 1
done

read -p "------------- Press enter to kill servers. --------------------"

kill $(ps aux | grep 'PlumtreeOpLogs.jar' | awk '{print $2}')