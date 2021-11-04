import datetime as dt
import numpy as np

results_file = "/home/evieira/finalResults/exp{}_{}_{}nodes_{}payload_{}prob_runs{}.csv"
file_name = "logs/{}nodes/{}/payload{}/prob{}/{}runs/node_{}_{}.log"
protocol = "plumtree"
HEADER = "avg_broadcast_latency, avg_bytes_received, avg_bytes_transmitted, avg_sent_vc, avg_sent_sync_ops, " \
         "avg_received_vc, avg_received_sync_ops, avg_sent_gossip, avg_sent_graft, avg_sent_prune, avg_sent_i_have, " \
         "avg_sent_send_vc, avg_sent_sync_gossip, avg_received_gossip, avg_received_dupes_gossip, avg_received_graft, " \
         "avg_received_prune, avg_received_i_have, avg_received_send_vc, avg_received_sync_gossip, " \
         "avg_received_dupes_sync_gossip"


def progress_bar(current, total, bar_length=20):
    percent = float(current) * 100 / total
    arrow = '-' * int(percent / 100 * bar_length - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))

    print('Progress: [%s%s] %d %%' % (arrow, spaces, percent), end='\r')


def parse_logs_plumtree(n_nodes, runs, payload, probability):
    n_runs = len(runs)
    for i in range(n_runs):
        runs[i] = int(runs[i])

    # Average Broadcast and Replication Latency
    msg_send_time = []
    msg_deliver_time = []
    msg_deliver_per_run = []

    # Average Bytes Received and Transmitted
    total_bytes_transmitted = 0
    total_bytes_received = 0

    # Number of Messages Sent and Received
    sent_vc = 0
    sent_sync_ops = 0
    received_vc = 0
    received_sync_ops = 0

    # Dupes Per Node and Interval
    dupes_per_run_node = np.zeros((n_nodes, n_runs))
    dupes_per_interval = []
    is_first = []
    start_time = []

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

    for _ in range(n_runs):
        dupes_per_interval.append([])
        is_first.append(True)
        msg_send_time.append({})
        msg_deliver_time.append({})
        msg_deliver_per_run.append({})

    for node in range(n_nodes):
        for run in range(n_runs):
            msg_deliver_per_run[run][node] = set(())

    last_msg_received = {}
    for node in range(n_nodes):
        progress_bar(node, n_nodes)
        for run in range(n_runs):

            # TODO: como abrir ficheiro node_1_1 etc
            f = open(start_name.format(n_nodes, protocol, payload, probability, runs[run], node), "r")

            final_bytes_transmitted = 0
            final_bytes_received = 0

            for i in f:
                line = i.split(" ")

                if is_first[run] == True:
                    if len(start_time) <= run:
                        start_time.append(dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f'))
                    else:
                        start_time[run] = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f')

                    is_first[run] = False

                # DUPES
                if line[3] == "DUPLICATE":
                    dupe_time = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f')
                    time_delta = dupe_time - start_time[run]
                    index = int(time_delta.total_seconds() / (float(interval) * 60))
                    while len(dupes_per_interval[run]) <= index:
                        dupes_per_interval[run].append(0)
                    dupes_per_interval[run][index] += 1

                # LATENCY
                elif line[3] == "SENT":
                    send_time_obj = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f').time()
                    msg_send_time[run][line[4]] = send_time_obj

                elif line[3] == "RECEIVED":
                    deliver_time_obj = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f').time()
                    last_msg_received[run] = dt.datetime.strptime(line[1], '%d/%m/%Y-%H:%M:%S,%f')
                    msg_id = line[4]

                    if msg_id not in msg_deliver_per_run[run][node]:
                        msg_deliver_per_run[run][node].add(msg_id)

                        if not msg_deliver_time[run].get(msg_id):
                            msg_deliver_time[run][msg_id] = deliver_time_obj

                        elif msg_deliver_time[run][msg_id] < deliver_time_obj:
                            msg_deliver_time[run][msg_id] = deliver_time_obj

                # BYTES COUNT
                elif final_bytes_received == 0 and len(line) >= 6 and "BytesSent" in line[5]:
                    final_bytes_transmitted = int(line[5].split("=")[1])
                    final_bytes_received = int(line[6].split("=")[1])

                # MESSAGE COUNTS
                elif line[3] == "sentVC:":
                    sent_vc += int(line[4])

                elif line[3] == "sentSyncOps:":
                    sent_sync_ops += int(line[4])

                elif line[3] == "receivedVC:":
                    received_vc += int(line[4])

                elif line[3] == "receivedSyncOps:":
                    received_sync_ops += int(line[4])

                elif line[3] == "sentGossip:":
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
                    dupes_per_run_node[node][run] += int(line[4])

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
                    dupes_per_run_node[node][run] += int(line[4])

            total_bytes_transmitted += final_bytes_transmitted
            total_bytes_received += final_bytes_received

        for run in range(n_runs):
            time_delta = last_msg_received[run] - start_time[run]
            index = int(time_delta.total_seconds() / (float(interval) * 60))
            while len(dupes_per_interval[run]) <= index:
                dupes_per_interval[run].append(0)

        np.savetxt(
            f"/home/evieira/finalResults/dupes_by_interval_{n_nodes}nodes_{protocol}_{probability}_{n_runs}runs.csv",
            np.array(dupes_per_interval), delimiter=",", fmt='%s')

    # BROADCAST LATENCY
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

    avg_bytes_received = total_bytes_received / n_runs
    avg_bytes_transmitted = total_bytes_transmitted / n_runs

    # MESSAGES STATS
    avg_sent_vc = sent_vc / n_runs
    avg_sent_sync_ops = sent_sync_ops / n_runs
    avg_received_vc = received_vc / n_runs
    avg_received_sync_ops = received_sync_ops / n_runs

    print("Progress: [------------------->] 100%", end='\n')

    np.savetxt(f"/home/evieira/finalResults/dupes_by_node_run_{n_nodes}_{protocol}_{probability}.csv",
               dupes_per_run_node, delimiter=",")

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

    results = int(avg_broadcast_latency), avg_bytes_received, avg_bytes_transmitted, avg_sent_vc, avg_sent_sync_ops, \
              avg_received_vc, avg_received_sync_ops, avg_sent_gossip, avg_sent_graft, avg_sent_prune, avg_sent_i_have, \
              avg_sent_send_vc, avg_sent_sync_gossip, avg_received_gossip, avg_received_dupes_gossip, avg_received_graft, \
              avg_received_prune, avg_received_i_have, avg_received_send_vc, avg_received_sync_gossip, \
              avg_received_dupes_sync_gossip

    res_str = ''
    for item in results:
        res_str = res_str + str(item) + ","
    print(res_str[:-1])
    with open(results_file.format(exp_name, protocol, node_number, payload, prob, runs_str), 'w') as fp:
        fp.write(HEADER)
        fp.write(res_str[:-1])
