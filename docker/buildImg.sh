#!/bin/sh

export DOCKER_BUILDKIT=1

docker build -f docker/Dockerfile --tag crdts-plumtree:exp.0.1 .

docker save -o docker/crdtsPlumtree.tar crdts-plumtree:exp.0.1
