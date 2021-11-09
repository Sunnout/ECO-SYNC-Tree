#!/bin/sh

python3 plotTotalDupesByPayload.py stable 200 flood,plumtree,periodicpull,periodicpullsmallertimer 128,1024 1 5 && \
python3 plotTotalDupesByProb.py stable 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 0.3,0.6,1 5 && \
python3 plotTotalDupes.py stable 50,100,200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotTotalDupes.py churn 50,100,200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotTotalDupes.py catastrophic_new 50,100,200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotTotalDupes.py catastrophic_dead 50,100,200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotTotalDupes.py stable 50,100,200 plumtree,plumtreegc 1024 1 5 && \
python3 plotTotalDupes.py churn 50,100,200 plumtree,plumtreegc 1024 1 5 && \
python3 plotTotalDupes.py catastrophic_new 50,100,200 plumtree,plumtreegc 1024 1 5 && \
python3 plotTotalDupes.py catastrophic_dead 50,100,200 plumtree,plumtreegc 1024 1 5
