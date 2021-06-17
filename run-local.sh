#!/bin/bash

processes=$1
newprocesses=$2
killprocesses=$3

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

i=0
port=5000

java -DlogFilename=results/results-$(hostname)-$[$port+$i] -jar target/PlumtreeOpLogs.jar -conf config.properties address=$(hostname) port=$port | sed "s/^/[$(($port + $i))] /"&
echo "launched contact on port $port"
sleep 1

i=1

while [ $i -lt $processes ]
do
	java -DlogFilename=results/results-$(hostname)-$[$port+$i] -jar target/PlumtreeOpLogs.jar -conf config.properties address=$(hostname) port=$[$port+$i] contact=$(hostname):$port | sed "s/^/[$(($port + $i))] /"&
	echo "launched process on port $[$port+$i]"
	i=$[$i+1]
	sleep 1
done

#Start from 1 so as not to kill contact node
k=1
sleep 15
while [ $k -le $killprocesses ]
do
  kill $(ps aux | grep "port=$[$port+$k]" | awk '{print $2}')
  k=$[$k+1]
  sleep 1
done

j=0
sleep 5
while [ $j -lt $newprocesses ]
do
  java -DlogFilename=results/results-$(hostname)-$[$port+$i] -jar target/PlumtreeOpLogs.jar -conf config-newnode.properties address=$(hostname) port=$[$port+$i] contact=$(hostname):$port | sed "s/^/[$(($port + $i))] /"&
  echo "launched process on port $[$port+$i]"
  i=$[$i+1]
  j=$[$j+1]
	sleep 1
done

read -p "------------- Press enter to kill servers. --------------------"

kill $(ps aux | grep 'PlumtreeOpLogs.jar' | awk '{print $2}')