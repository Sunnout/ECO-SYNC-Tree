#!/bin/bash

off=$1
mark=$2
config=$3
image=$4
bandwidth=$5
net=$6
vol_logs=$7
vol_data=$8
latencyMap=$9

function cmd {
    echo $1
    eval $1
}

i=-1
echo "Launching containers...  off=$off mark=$mark"
while read -r ip
do
  i=$((i+1))

  if [ $i -lt $off ]; then
    continue
  elif [ $i -ge $mark ]; then
    break
  fi

  cmd "docker run --rm -v /lib/modules:/lib/modules -v ${vol_logs}:/logs -v ${vol_data}:/data -d -t --cap-add=NET_ADMIN --net $net --ip $ip --name node_$i -h node_$i $image $i $bandwidth $latencyMap"
  echo "${i}. Container node_$i with ip $ip launched"

done < "$config"
