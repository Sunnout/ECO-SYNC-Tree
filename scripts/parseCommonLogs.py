from glob import glob
import os
import datetime as dt
from parseOutputFile import parse_output
import math


def progressBar(current, total, barLength = 20):
    percent = float(current) * 100 / total
    arrow = '-' * int(percent/100 * barLength - 1) + '>'
    spaces = ' ' * (barLength - len(arrow))

    print('Progress: [%s%s] %d %%' % (arrow, spaces, percent), end='\r')


def parse_common_logs(run_paths):
    bcast_latencies_per_run = {}
    bytes_per_second = []
    dupes_per_second = []
    sync_time_per_second = []
    disk_usage_per_second = []
    sync_start_times = {}
    sync_end_times = {}

    for run_path in run_paths:  # RUNS
        node_files = glob(run_path + "/*.log")
        print(f"Found {len(node_files)} node files for {os.path.basename(run_path)}")

        send_times = {}
        reception_times = {}

        run_start_time, first_dead_time, first_cooldown_time, first_message_time, last_start_time, catastrophe_start_time, \
            churn_start_time, churn_end_time = parse_output(run_path)

        if len(bytes_per_second) == 0:
            for _ in range(math.ceil(first_dead_time)+1):
                bytes_per_second.append(0)
                dupes_per_second.append(0)
                sync_time_per_second.append((0, 0))
                disk_usage_per_second.append((0, 0))

        idx = 0
        for node_file in node_files:  # NODES
            progressBar(idx, len(node_files))
            idx += 1
            file = open(node_file, "r")
            start_time = -1

            for i in file:
                line = i.split(" ")

                if "Hello" in line[3]:
                    start_time = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f').timestamp() - run_start_time

                # BROADCAST LATENCY
                elif line[3] == "SENT":
                    send_times[line[4]] = dt.datetime.strptime(line[1],
                                                               '%d/%m/%Y-%H:%M:%S,%f').timestamp() - run_start_time
                    if send_times[line[4]] > first_dead_time:
                        print(f"Erro não convergiu {line[4]}: send time {send_times[line[4]]}; first dead {first_dead_time}")
                        exit()

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

                # SYNC TIMES
                elif line[3] == "STARTED_SYNC":
                    sync_start_times[line[4]] = dt.datetime.strptime(line[1],
                                                               '%d/%m/%Y-%H:%M:%S,%f').timestamp() - run_start_time

                elif line[3] == "ENDED_SYNC":
                    sync_end_times[line[4]] = dt.datetime.strptime(line[1],
                                                                     '%d/%m/%Y-%H:%M:%S,%f').timestamp() - run_start_time

                # DISK USAGE
                elif "DiskUsage" in line[3]:
                    disk_usage_time = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f').timestamp() - run_start_time
                    if disk_usage_time < first_dead_time:
                        n_print_sec, disk_usage_sec = disk_usage_per_second[math.floor(disk_usage_time)]
                        disk_usage_per_second[math.floor(disk_usage_time)] = (n_print_sec + 1, disk_usage_sec + int(line[3].split("=")[1]))

        # AFTER NODES

        # BROADCAST LATENCY
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

    # SYNC TIMES
    for msg_id, start_time in sync_start_times.items():
        if start_time < first_dead_time and msg_id in sync_end_times:
            n_sync_sec, sync_time_sec = sync_time_per_second[math.floor(start_time)]
            sync_time_per_second[math.floor(start_time)] = (n_sync_sec + 1, sync_time_sec + sync_end_times[msg_id] - start_time)

    avg_sync_time_per_second = []
    avg_n_syncs_per_second = []
    total_sync_time = 0
    n_syncs = 0
    for n_sync_sec, sync_time_sec in sync_time_per_second:
        total_sync_time += sync_time_sec
        n_syncs += n_sync_sec
        if n_sync_sec == 0:
            avg_sync_time_per_second.append(0)
        else:
            avg_sync_time_per_second.append(sync_time_sec / n_sync_sec)
        avg_n_syncs_per_second.append(n_sync_sec / len(run_paths))

    if n_syncs > 0:
        avg_sync_time = total_sync_time / n_syncs
    else:
        avg_sync_time = -1
    total_sync_time = total_sync_time / len(run_paths)
    n_syncs = n_syncs / len(run_paths)

    # DISK USAGE
    avg_disk_usage_per_second = []
    for n_print_sec, disk_usage_sec in disk_usage_per_second:
        if n_print_sec == 0:
            avg_disk_usage_per_second.append(0)
        else:
            avg_disk_usage_per_second.append(disk_usage_sec / n_print_sec)

    # TREE STABILIZATION IN CATASTROPHE
    tree_stabilization_time = -1
    last_idx = -1
    if catastrophe_start_time != -1:
        for idx, n_syncs in enumerate(avg_n_syncs_per_second):
            if n_syncs != 0:
                last_idx = idx
        tree_stabilization_time = last_idx - catastrophe_start_time

    return {"AVG_BCAST_LATENCY": avg_broadcast_latency,
            "AVG_LATENCIES_PER_SECOND": avg_latencies_per_second,
            "TOTAL_BYTES": total_bytes,
            "TOTAL_BYTES_PER_SECOND": total_bytes_per_second,
            "TOTAL_DUPES": total_dupes,
            "TOTAL_DUPES_PER_SECOND": total_dupes_per_second,
            "AVG_SYNC_TIME_PER_SECOND": avg_sync_time_per_second,
            "AVG_N_SYNCS_PER_SECOND": avg_n_syncs_per_second,
            "AVG_SYNC_TIME": avg_sync_time,
            "N_SYNCS": n_syncs,
            "TOTAL_SYNC_TIME": total_sync_time,
            "AVG_DISK_USAGE_PER_SECOND": avg_disk_usage_per_second,
            "TREE_STABILIZATION_TIME": tree_stabilization_time,
            "FIRST_NODE_DEAD": first_dead_time,
            "FIRST_NODE_COOLDOWN": first_cooldown_time,
            "FIRST_MESSAGE": first_message_time,
            "LAST_NODE_START": last_start_time,
            "START_CATASTROPHE": catastrophe_start_time,
            "START_CHURN": churn_start_time,
            "END_CHURN": churn_end_time}