#!/bin/sh

python3 plotAverageBroadcastLatencyByPayload.py stable 200 flood,plumtree,periodicpull,periodicpullsmallertimer 128,1024 1 5 && \
python3 plotAverageBroadcastLatencyByProb.py stable 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 0.3,0.6,1 5 && \
python3 plotAverageBroadcastLatency.py stable 50,100,200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotAverageBroadcastLatency.py churn 50,100,200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotAverageBroadcastLatency.py catastrophic_new 50,100,200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotAverageBroadcastLatency.py catastrophic_dead 50,100,200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotAverageBroadcastLatency.py stable 50,100,200 plumtree,plumtreegc 1024 1 5 && \
python3 plotAverageBroadcastLatency.py churn 50,100,200 plumtree,plumtreegc 1024 1 5 && \
python3 plotAverageBroadcastLatency.py catastrophic_new 50,100,200 plumtree,plumtreegc 1024 1 5 && \
python3 plotAverageBroadcastLatency.py catastrophic_dead 50,100,200 plumtree,plumtreegc 1024 1 5
