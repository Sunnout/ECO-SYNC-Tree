#!/bin/bash

off=$1
mark=$2
config=$3
image=$4
bandwidth=$5
net=$6
vol=$7

function cmd {
    echo $1
    eval $1
}

i=-1
echo "Lauching containers...  off=$off mark=$mark"
while read -r ip
do
  i=$((i+1))

  if [ $i -lt $off ]; then
    continue
  elif [ $i -ge $mark ]; then
    break
  fi

  cmd "docker run --rm -v /lib/modules:/lib/modules -v ${vol}:/logs -d -t --cap-add=NET_ADMIN --net $net --ip $ip --name node_$i -h node_$i $image $i $bandwidth"
  echo "${i}. Container node_$i with ip $ip lauched"

done < "$config"
