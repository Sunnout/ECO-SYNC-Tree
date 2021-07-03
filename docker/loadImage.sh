#!/bin/bash

for node in $(oarprint host); do
  oarsh $node "docker load -i '$(pwd)'/crdtsPlumtree.tar" &
done
wait
