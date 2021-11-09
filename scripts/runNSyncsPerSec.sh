#!/bin/sh

python3 plotNumberSyncsPerSecond.py stable 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotNumberSyncsPerSecond.py stable 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotNumberSyncsPerSecond.py stable 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotNumberSyncsPerSecond.py churn 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotNumberSyncsPerSecond.py churn 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotNumberSyncsPerSecond.py churn 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotNumberSyncsPerSecond.py catastrophic_new 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotNumberSyncsPerSecond.py catastrophic_new 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotNumberSyncsPerSecond.py catastrophic_new 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotNumberSyncsPerSecond.py catastrophic_dead 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotNumberSyncsPerSecond.py catastrophic_dead 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotNumberSyncsPerSecond.py catastrophic_dead 200 plumtree,plumtreegc 1024 1 5

