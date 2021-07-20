import os
import datetime as dt

def parse_logs(start_name, n_processes, runs, protocol, probability, interval):
    n_runs=len(runs)
    i = 0
    for i in range(len(runs)):
        runs[i] = int(runs[i])

    #Average Broadcast and Replication Latency
    msg_send_time = []
    msg_deliver_time = []
    msg_deliver_per_run = []
    msg_gen_time = []
    msg_exec_time = []
    msg_exec_per_run = []

    #Average Bytes Received and Transmitted
    total_bytes_transmitted = 0
    total_bytes_received = 0

    #Number of Messages Sent and Received
    sent_vc = 0
    sent_sync_ops = 0
    received_vc = 0
    received_sync_ops = 0

    if protocol == "plumtree":
        sent_gossip = 0
        sent_graft = 0
        sent_prune = 0
        sent_i_have = 0
        sent_send_vc = 0
        sent_sync_gossip = 0
        received_gossip = 0
        received_dupes_gossip = 0
        received_graft = 0
        received_prune = 0
        received_i_have = 0
        received_send_vc = 0
        received_sync_gossip = 0
        received_dupes_sync_gossip = 0

    elif protocol == "flood":
        sent_flood = 0
        sent_send_vc = 0
        sent_sync_flood = 0
        received_flood = 0
        received_dupes_flood = 0
        received_send_vc = 0
        received_sync_flood = 0
        received_dupes_sync_flood = 0

    elif protocol == "periodicpull":
        sent_sync_pull = 0
        received_sync_pull = 0
        received_dupes_pull = 0

    for run in range(n_runs):
        msg_send_time.append({})
        msg_deliver_time.append({})
        msg_deliver_per_run.append({})
        msg_gen_time.append({})
        msg_exec_time.append({})
        msg_exec_per_run.append({})

    for proc in range(n_processes):
        for run in range(n_runs):
            msg_deliver_per_run[run][proc] = set(())
            msg_exec_per_run[run][proc] = set(())

    for proc in range(n_processes):
        progressBar(proc, n_processes)
        for run in range(n_runs):
            f = open(start_name.format(n_processes, protocol, probability, runs[run], proc), "r")
#             f = open("../results-chiclet-7.lille.grid5000.fr-5010.txt", "r")

            final_bytes_transmitted = 0
            final_bytes_received = 0

            for i in f:
                line = i.split(" ")
                #guardar start time da exp se for 1º linha do 1º ficheiro e depois usar interval para acumular dupes

                #LATENCY
                if line[3] == "SENT":
                    send_time_obj = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f').time()
                    msg_send_time[run][line[4]] = send_time_obj

                elif line[3] == "RECEIVED":
                    deliver_time_obj = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f').time()
                    msg_id = line[4]

                    if msg_id not in msg_deliver_per_run[run][proc]:
                        msg_deliver_per_run[run][proc].add(msg_id)

                        if not msg_deliver_time[run].get(msg_id):
                            msg_deliver_time[run][msg_id] = deliver_time_obj

                        elif msg_deliver_time[run][msg_id] < deliver_time_obj:
                            msg_deliver_time[run][msg_id] = deliver_time_obj

                elif line[3] == "GENERATED":
                    gen_time_obj = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f').time()
                    msg_gen_time[run][line[4]] = gen_time_obj

                elif line[3] == "EXECUTED":
                    exec_time_obj = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f').time()
                    msg_id = line[4]
                    final_bytes_transmitted = 0
                    final_bytes_received = 0

                    if msg_id not in msg_exec_per_run[run][proc]:
                        msg_exec_per_run[run][proc].add(msg_id)

                        if not msg_exec_time[run].get(msg_id):
                            msg_exec_time[run][msg_id] = exec_time_obj

                        elif msg_exec_time[run][msg_id] < exec_time_obj:
                            msg_exec_time[run][msg_id] = exec_time_obj

                #BYTES COUNT
                elif final_bytes_received == 0 and len(line) >= 6 and "BytesSent" in line[5]:
                    final_bytes_transmitted = int(line[5].split("=")[1])
                    final_bytes_received = int(line[6].split("=")[1])

                #MESSAGE COUNTS
                #mydupes[proc][run][proto] = gossipDupes + syncDupes talvez guardar num file por proto com linhas processos e coluna run

                elif line[3] == "sentVC:":
                    sent_vc += int(line[4])

                elif line[3] == "sentSyncOps:":
                    sent_sync_ops += int(line[4])

                elif line[3] == "receivedVC:":
                    received_vc += int(line[4])

                elif line[3] == "receivedSyncOps:":
                    received_sync_ops += int(line[4])

                elif protocol == "plumtree":
                    if line[3] == "sentGossip:":
                        sent_gossip += int(line[4])

                    elif line[3] == "sentIHave:":
                        sent_i_have += int(line[4])

                    elif line[3] == "sentGraft:":
                        sent_graft += int(line[4])

                    elif line[3] == "sentPrune:":
                        sent_prune += int(line[4])

                    elif line[3] == "sentSendVC:":
                        sent_send_vc += int(line[4])

                    elif line[3] == "sentSyncGossip:":
                        sent_sync_gossip += int(line[4])

                    elif line[3] == "receivedGossip:":
                        received_gossip += int(line[4])

                    elif line[3] == "receivedDupesGossip:":
                        received_dupes_gossip += int(line[4])

                    elif line[3] == "receivedIHave:":
                        received_i_have += int(line[4])

                    elif line[3] == "receivedGraft:":
                        received_graft += int(line[4])

                    elif line[3] == "receivedPrune:":
                        received_prune += int(line[4])

                    elif line[3] == "receivedSendVC:":
                        received_send_vc += int(line[4])

                    elif line[3] == "receivedSyncGossip:":
                        received_sync_gossip += int(line[4])

                    elif line[3] == "receivedDupesSyncGossip:":
                        received_dupes_sync_gossip += int(line[4])

                elif protocol == "flood":
                    if line[3] == "sentFlood:":
                        sent_flood += int(line[4])

                    elif line[3] == "sentSendVC:":
                        sent_send_vc += int(line[4])

                    elif line[3] == "sentSyncFlood:":
                        sent_sync_flood += int(line[4])

                    elif line[3] == "receivedFlood:":
                        received_flood += int(line[4])

                    elif line[3] == "receivedDupesFlood:":
                        received_dupes_flood += int(line[4])

                    elif line[3] == "receivedSendVC:":
                        received_send_vc += int(line[4])

                    elif line[3] == "receivedSyncFlood:":
                        received_sync_flood += int(line[4])

                    elif line[3] == "receivedDupesSyncFlood:":
                        received_dupes_sync_flood += int(line[4])

                elif protocol == "periodicpull":
                    if line[3] == "sentSyncPull:":
                        sent_sync_pull += int(line[4])

                    elif line[3] == "receivedSyncPull:":
                        received_sync_pull += int(line[4])

                    elif line[3] == "receivedDupes:":
                        received_dupes_pull += int(line[4])

            total_bytes_transmitted += final_bytes_transmitted
            total_bytes_received += final_bytes_received

    #LATENCY BROADCAST LAYER
    latency = []

    for run in range(n_runs):
        latency.append({})
        for key in msg_send_time[run]:
            if msg_deliver_time[run].get(key):
                deliver_date = dt.datetime.combine(dt.date.today(), msg_deliver_time[run][key])
                send_date = dt.datetime.combine(dt.date.today(), msg_send_time[run][key])
                latency[run][key] = deliver_date - send_date

    total_time = 0
    total_messages = 0

    for run in range(n_runs):
        for key in latency[run]:
            total_time += latency[run][key].total_seconds()
        total_messages += len(latency[run])

    avg_broadcast_latency = (total_time / total_messages) * 1000

    #LATENCY REPLICATION LAYER
    latency = []

    for run in range(n_runs):
        latency.append({})
        for key in msg_gen_time[run]:
            if msg_exec_time[run].get(key):
                exec_date = dt.datetime.combine(dt.date.today(), msg_exec_time[run][key])
                gen_date = dt.datetime.combine(dt.date.today(), msg_gen_time[run][key])
                latency[run][key] = exec_date - gen_date

    total_time = 0
    total_messages = 0

    for run in range(n_runs):
        for key in latency[run]:
            total_time += latency[run][key].total_seconds()
        total_messages += len(latency[run])

    avg_replication_latency = (total_time / total_messages) * 1000

    avg_bytes_received = total_bytes_received/ n_runs
    avg_bytes_transmitted = total_bytes_transmitted / n_runs

    #MESSAGES STATS
    avg_sent_vc = sent_vc / n_runs
    avg_sent_sync_ops = sent_sync_ops / n_runs
    avg_received_vc = received_vc / n_runs
    avg_received_sync_ops = received_sync_ops / n_runs

    print("Progress: [------------------->] 100%", end='\n')

    if protocol == "plumtree":
        avg_sent_gossip = sent_gossip / n_runs
        avg_sent_graft = sent_graft / n_runs
        avg_sent_prune = sent_prune / n_runs
        avg_sent_i_have = sent_i_have / n_runs
        avg_sent_send_vc = sent_send_vc / n_runs
        avg_sent_sync_gossip = sent_sync_gossip / n_runs
        avg_received_gossip = received_gossip / n_runs
        avg_received_dupes_gossip = received_dupes_gossip / n_runs
        avg_received_graft = received_graft / n_runs
        avg_received_prune = received_prune / n_runs
        avg_received_i_have = received_i_have / n_runs
        avg_received_send_vc = received_send_vc / n_runs
        avg_received_sync_gossip = received_sync_gossip / n_runs
        avg_received_dupes_sync_gossip = received_dupes_sync_gossip / n_runs
        return int(avg_broadcast_latency), int(avg_replication_latency), avg_bytes_received, avg_bytes_transmitted, avg_sent_vc, avg_sent_sync_ops, avg_received_vc, avg_received_sync_ops, avg_sent_gossip, avg_sent_graft, avg_sent_prune, avg_sent_i_have, avg_sent_send_vc, avg_sent_sync_gossip, avg_received_gossip, avg_received_dupes_gossip, avg_received_graft, avg_received_prune, avg_received_i_have, avg_received_send_vc, avg_received_sync_gossip, avg_received_dupes_sync_gossip

    elif protocol == "flood":
        avg_sent_flood = sent_flood / n_runs
        avg_sent_send_vc = sent_send_vc / n_runs
        avg_sent_sync_flood = sent_sync_flood / n_runs
        avg_received_flood = received_flood / n_runs
        avg_received_dupes_flood = received_dupes_flood / n_runs
        avg_received_send_vc = received_send_vc / n_runs
        avg_received_sync_flood = received_sync_flood / n_runs
        avg_received_dupes_sync_flood = received_dupes_sync_flood / n_runs
        return int(avg_broadcast_latency), int(avg_replication_latency), avg_bytes_received, avg_bytes_transmitted, \
               avg_sent_vc, avg_sent_sync_ops, avg_received_vc, avg_received_sync_ops, avg_sent_flood, avg_sent_send_vc, \
               avg_sent_sync_flood, avg_received_flood, avg_received_dupes_flood, avg_received_send_vc, avg_received_sync_flood, avg_received_dupes_sync_flood

    elif protocol == "periodicpull":
        avg_sent_sync_pull = sent_sync_pull / n_runs
        avg_received_sync_pull = received_sync_pull / n_runs
        avg_received_dupes_pull = received_dupes_pull / n_runs
        return int(avg_broadcast_latency), int(avg_replication_latency), avg_bytes_received, avg_bytes_transmitted, avg_sent_vc, avg_sent_sync_ops, avg_received_vc, avg_received_sync_ops, avg_sent_sync_pull, avg_received_sync_pull, avg_received_dupes_pull

def progressBar(current, total, barLength = 20):
    percent = float(current) * 100 / total
    arrow   = '-' * int(percent/100 * barLength - 1) + '>'
    spaces  = ' ' * (barLength - len(arrow))

    print('Progress: [%s%s] %d %%' % (arrow, spaces, percent), end='\r')