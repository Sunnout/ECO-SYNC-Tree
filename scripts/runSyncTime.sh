#!/bin/sh

python3 plotTotalSyncTime.py stable 50,100,200 plumtree,plumtreegc 1024 1 5 && \
python3 plotTotalSyncTime.py churn 50,100,200 plumtree,plumtreegc 1024 1 5 && \
python3 plotTotalSyncTime.py catastrophic_new 50,100,200 plumtree,plumtreegc 1024 1 5 && \
python3 plotTotalSyncTime.py catastrophic_dead 50,100,200 plumtree,plumtreegc 1024 1 5

