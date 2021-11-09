#!/bin/sh

python3 plotDupesPerSecond.py stable 50 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecond.py stable 100 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecond.py stable 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecond.py churn 50 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecond.py churn 100 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecond.py churn 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecond.py catastrophic_new 50 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecond.py catastrophic_new 100 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecond.py catastrophic_new 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecond.py catastrophic_dead 50 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecond.py catastrophic_dead 100 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecond.py catastrophic_dead 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecondChurn.py churn 50 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecondChurn.py churn 100 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecondChurn.py churn 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecondCat.py catastrophic_new 50 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecondCat.py catastrophic_new 100 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecondCat.py catastrophic_new 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecondCat.py catastrophic_dead 50 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecondCat.py catastrophic_dead 100 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecondCat.py catastrophic_dead 200 flood,plumtree,periodicpull,periodicpullsmallertimer 1024 1 5 && \
python3 plotDupesPerSecond.py stable 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecond.py stable 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecond.py stable 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecond.py churn 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecond.py churn 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecond.py churn 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecond.py catastrophic_new 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecond.py catastrophic_new 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecond.py catastrophic_new 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecond.py catastrophic_dead 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecond.py catastrophic_dead 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecond.py catastrophic_dead 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecondChurn.py churn 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecondChurn.py churn 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecondChurn.py churn 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecondCat.py catastrophic_new 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecondCat.py catastrophic_new 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecondCat.py catastrophic_new 200 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecondCat.py catastrophic_dead 50 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecondCat.py catastrophic_dead 100 plumtree,plumtreegc 1024 1 5 && \
python3 plotDupesPerSecondCat.py catastrophic_dead 200 plumtree,plumtreegc 1024 1 5

