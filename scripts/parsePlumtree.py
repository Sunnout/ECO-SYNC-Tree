from glob import glob
import os
import datetime as dt
from parseOutputFile import parse_output
import math


def parse_logs_plumtree(run_paths):
    bcast_latencies_per_run = {}
    bytes_per_second = []
    dupes_per_second = []

    for run_path in run_paths:  # RUNS
        node_files = glob(run_path + "/*.log")
        print(f"Found {len(node_files)} node files for {os.path.basename(run_path)}")

        send_times = {}
        reception_times = {}

        run_start_time, first_dead_time, last_start_time, catastrophe_start_time, \
            churn_start_time, churn_end_time = parse_output(run_path)

        if len(bytes_per_second) == 0:
            for _ in range(math.ceil(first_dead_time)):
                bytes_per_second.append(0)
                dupes_per_second.append(0)

        for node_file in node_files:  # NODES
            file = open(node_file, "r")
            start_time = -1

            for i in file:
                line = i.split(" ")

                if line[3] == "Hello":
                    start_time = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f').timestamp() - run_start_time

                # BROADCAST LATENCY
                elif line[3] == "SENT":
                    send_times[line[4]] = dt.datetime.strptime(line[1],
                                                               '%d/%m/%Y-%H:%M:%S,%f').timestamp() - run_start_time

                elif line[3] == "RECEIVED":
                    reception_time = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f').timestamp() - run_start_time
                    msg_id = line[4]
                    if msg_id not in reception_times:
                        reception_times[msg_id] = []
                    reception_times[msg_id].append((start_time, reception_time))

                # BYTES
                elif len(line) > 6 and "BytesSent" in line[5]:
                    bytes_time = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f').timestamp() - run_start_time
                    if bytes_time < first_dead_time:
                        bytes_per_second[math.floor(bytes_time)] += int(line[5].split("=")[1])

                # DUPLICATES
                elif line[3] == "DUPLICATE":
                    dupe_time = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f').timestamp() - run_start_time
                    if dupe_time < first_dead_time:
                        dupes_per_second[math.floor(dupe_time)] += 1

        # AFTER NODES
        broadcast_latencies = []
        for msg_id, send_time in send_times.items():
            last_reception = -1

            for start_time, reception_time in reception_times[msg_id]:
                if start_time < send_time and reception_time > last_reception:
                    last_reception = reception_time

            broadcast_latencies.append((send_time, last_reception - send_time))

        bcast_latencies_per_run[run_path] = broadcast_latencies

    # AFTER RUNS

    # BROADCAST LATENCY
    n_latencies = 0
    total_latency = 0
    for run_latencies in bcast_latencies_per_run.values():
        for send_time, bcast_latency in run_latencies:
            n_latencies += 1
            total_latency += bcast_latency
    avg_broadcast_latency = total_latency / n_latencies
    latencies_per_second = []
    for _ in range(math.ceil(first_dead_time)):
        latencies_per_second.append((0, 0))
    for run_latencies in bcast_latencies_per_run.values():
        for send_time, bcast_latency in run_latencies:
            if send_time > first_dead_time:
                print("Erro não convergiu")
                exit()
            n_ops_sec, bcast_latency_sec = latencies_per_second[math.floor(send_time)]
            latencies_per_second[math.floor(send_time)] = (n_ops_sec + 1, bcast_latency_sec + bcast_latency)
    avg_latencies_per_second = []
    for n_ops_sec, bcast_latency_sec in latencies_per_second:
        if n_ops_sec == 0:
            avg_latencies_per_second.append(0)
        else:
            avg_latencies_per_second.append(bcast_latency_sec/n_ops_sec)

    # BYTES
    total_bytes_per_second = []
    for bytes_sec in bytes_per_second:
        total_bytes_per_second.append(bytes_sec/len(run_paths))
    total_bytes = sum(total_bytes_per_second) / len(run_paths)

    # DUPLICATES
    total_dupes_per_second = []
    for dupes_sec in dupes_per_second:
        total_dupes_per_second.append(dupes_sec / len(run_paths))
    total_dupes = sum(total_dupes_per_second) / len(run_paths)


    return {"AVG_BCAST_LATENCY": avg_broadcast_latency,
            "AVG_LATENCIES_PER_SECOND": avg_latencies_per_second,
            "TOTAL_BYTES": total_bytes,
            "TOTAL_BYTES_PER_SECOND": total_bytes_per_second,
            "TOTAL_DUPES": total_dupes,
            "TOTAL_DUPES_PER_SECOND": total_dupes_per_second,
            "FIRST_NODE_DEAD": first_dead_time,
            "LAST_NODE_START": last_start_time,
            "START_CATASTROPHE": catastrophe_start_time,
            "START_CHURN": churn_start_time,
            "END_CHURN": churn_end_time}