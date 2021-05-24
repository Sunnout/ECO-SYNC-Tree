#!/bin/bash

processes=$1

if [ -z $processes ] || [ $processes -lt 1 ]; then
	        echo "please indicate a number of processes of at least one"
		        exit 0
fi

i=0
port=5000

(java -DlogFilename=results/results-$(hostname)-$[$port+$i] -jar target/PlumtreeOpLogs.jar -conf config.properties address=$(hostname) port=$port | sed "s/^/[$(($port + $i))] /")&
echo "launched contact on port $port"
sleep 1


i=1

while [ $i -lt $processes ]
do
	(java -DlogFilename=results/results-$(hostname)-$[$port+$i] -jar target/PlumtreeOpLogs.jar -conf config.properties address=$(hostname) port=$[$port+$i] contact=$(hostname):$port | sed "s/^/[$(($port + $i))] /")&
	echo "launched process on port $[$port+$i]"
	i=$[$i+1]
	sleep 1
done

#sleep 40
#(java -DlogFilename=results/results-$(hostname)-$[$port+$i] -jar target/PlumtreeOpLogs.jar -conf config-newnode.properties address=$(hostname) port=$[$port+$i] contact=$(hostname):$port | sed "s/^/[$(($port + $i))] /")&
#echo "launched process on port $[$port+$i]"

read -p "------------- Press enter to kill servers. --------------------"

kill $(ps aux | grep 'PlumtreeOpLogs.jar' | awk '{print $2}')