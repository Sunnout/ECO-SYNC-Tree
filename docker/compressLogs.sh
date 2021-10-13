#!/bin/sh

expName=$1
host=$2

tar -czvf $HOME/$expName-$host.tar.gz /tmp/logs
