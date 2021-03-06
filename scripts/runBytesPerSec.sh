#!/bin/sh

python3 plotBytesPerSecond.py stable 50 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecond.py stable 100 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecond.py stable 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecond.py churn 50 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecond.py churn 100 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecond.py churn 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecond.py catastrophic_new 50 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecond.py catastrophic_new 100 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecond.py catastrophic_new 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecond.py catastrophic_dead 50 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecond.py catastrophic_dead 100 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecond.py catastrophic_dead 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecondChurn.py churn 50 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecondChurn.py churn 100 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecondChurn.py churn 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecondCat.py catastrophic_new 50 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecondCat.py catastrophic_new 100 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecondCat.py catastrophic_new 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecondCat.py catastrophic_dead 50 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecondCat.py catastrophic_dead 100 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecondCat.py catastrophic_dead 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotBytesPerSecond.py stable 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecond.py stable 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecond.py stable 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecond.py churn 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecond.py churn 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecond.py churn 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecond.py catastrophic_new 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecond.py catastrophic_new 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecond.py catastrophic_new 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecond.py catastrophic_dead 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecond.py catastrophic_dead 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecond.py catastrophic_dead 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecondChurn.py churn 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecondChurn.py churn 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecondChurn.py churn 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecondCat.py catastrophic_new 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecondCat.py catastrophic_new 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecondCat.py catastrophic_new 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecondCat.py catastrophic_dead 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecondCat.py catastrophic_dead 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotBytesPerSecondCat.py catastrophic_dead 200 plumtree,plumtreegc 1024 1 5
