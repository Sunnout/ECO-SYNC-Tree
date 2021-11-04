import datetime as dt
import numpy as np
import sys


def parse_logs(file_name):

    #Sync start and end times
    sync_end_time = {}
    sync_start_time = {}

    f = open(file_name, "r")
    for i in f:
        line = i.split(" ")

        #END TIME
        if line[3] == "ENDED_SYNC":
            end_time_obj = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f').time()
            sync_end_time[line[4]] = end_time_obj

        #START TIME
        elif line[3] == "STARTED_SYNC":
            start_time_obj = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f').time()
            sync_start_time[line[4]] = start_time_obj

    # Sync Latency
    sync_latency = {}

    for key in sync_start_time:
        if sync_end_time.get(key):
            end_date = dt.datetime.combine(dt.date.today(), sync_end_time[key])
            start_date = dt.datetime.combine(dt.date.today(), sync_start_time[key])
            sync_latency[key] = (end_date - start_date).total_seconds()

    with open('syncTimes200_prob1_1new.txt', 'w') as f:
        #print(sync_latency, file=f)
        for k, v in sync_latency.items():
            f.write(str(k) + ' >>> '+ str(v) + '\n')

file = sys.argv[1]
parse_logs(file)

