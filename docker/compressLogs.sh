#!/bin/sh


host=$1
nodes=$2
runs=$3
protos=$4
probs=$5

tar -czvf $HOME/$host-$nodes-$protos-$probs-$runs.tar.gz /tmp/logs
