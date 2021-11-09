#!/bin/sh

python3 plotSyncTimePerSecond.py stable 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotSyncTimePerSecond.py stable 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotSyncTimePerSecond.py stable 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotSyncTimePerSecond.py churn 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotSyncTimePerSecond.py churn 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotSyncTimePerSecond.py churn 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotSyncTimePerSecond.py catastrophic_new 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotSyncTimePerSecond.py catastrophic_new 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotSyncTimePerSecond.py catastrophic_new 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotSyncTimePerSecond.py catastrophic_dead 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotSyncTimePerSecond.py catastrophic_dead 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotSyncTimePerSecond.py catastrophic_dead 200 plumtree,plumtreegc 1024 1 5

