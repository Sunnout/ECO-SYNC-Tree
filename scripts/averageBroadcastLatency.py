import datetime as dt

def avg_latency(start_name, n_processes, n_runs, protocol, workload, to_print=False):
    msg_send_time = []
    msg_deliver_time = []
    msg_deliver_per_run = []
    msg_gen_time = []
    msg_exec_time = []
    msg_exec_per_run = []

    #Duplicated messages

    for run in range(n_runs):
        msg_send_time.append({})
        msg_deliver_time.append({})
        msg_deliver_per_run.append({})
        msg_gen_time.append({})
        msg_exec_time.append({})
        msg_exec_per_run.append({})

    for proc in range(n_processes):
        for run in range(n_runs):
            msg_deliver_per_run[run][proc] = []
            msg_exec_per_run[run][proc] = []

    for proc in range(n_processes):
        progressBar(proc, n_processes)
        for run in range(n_runs):
            #f = open(start_name.format(proc, protocol, workload, run+1), "r")
            f = open(start_name.format(protocol + proc), "r")
            
            for i in f:
                line = i.split(" ")
                if line[2].__contains__("SENT"):
                    send_time = line[0].split("[")[1][:-1]
                    send_time_obj = dt.datetime.strptime(send_time, '%H:%M:%S,%f').time()
                    msg_send_time[run][line[3]] = send_time_obj

                elif line[2].__contains__("RECEIVED"):
                    deliver_time = line[0].split("[")[1][:-1]
                    deliver_time_obj = dt.datetime.strptime(deliver_time, '%H:%M:%S,%f').time()
                    msg_id = line[3]

                    if msg_id not in msg_deliver_per_run[run][proc]:
                        msg_deliver_per_run[run][proc].append(msg_id)

                        if not msg_deliver_time[run].get(msg_id):
                            msg_deliver_time[run][msg_id] = deliver_time_obj

                        elif msg_deliver_time[run][msg_id] < deliver_time_obj:
                            msg_deliver_time[run][msg_id] = deliver_time_obj

                elif line[2].__contains__("GENERATED"):
                    gen_time = line[0].split("[")[1][:-1]
                    gen_time_obj = dt.datetime.strptime(gen_time, '%H:%M:%S,%f').time()
                    msg_gen_time[run][line[3]] = gen_time_obj

                elif line[2].__contains__("EXECUTED"):
                    exec_time = line[0].split("[")[1][:-1]
                    exec_time_obj = dt.datetime.strptime(exec_time, '%H:%M:%S,%f').time()
                    msg_id = line[3]

                    if msg_id not in msg_exec_per_run[run][proc]:
                        msg_exec_per_run[run][proc].append(msg_id)

                        if not msg_exec_time[run].get(msg_id):
                            msg_exec_time[run][msg_id] = exec_time_obj

                        elif msg_exec_time[run][msg_id] < exec_time_obj:
                            msg_exec_time[run][msg_id] = exec_time_obj

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
    print("Progress: [------------------->] 100%", end='\n')

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

    if(to_print):
        print("Average Broadcast Latency: {:.0f} ms".format(avg_broadcast_latency))
        print("Average Replication Latency: {:.0f} ms".format(avg_replication_latency))
        print("")
        
    return avg_broadcast_latency, avg_replication_latency

def progressBar(current, total, barLength = 20):
    percent = float(current) * 100 / total
    arrow   = '-' * int(percent/100 * barLength - 1) + '>'
    spaces  = ' ' * (barLength - len(arrow))

    print('Progress: [%s%s] %d %%' % (arrow, spaces, percent), end='\r')


start_name = "../logs/results-chiclet-7.lille.grid5000.fr-{}.log"
n_processes = 3
starting_port = 5000
n_runs=1

avg_latency(start_name, n_processes, n_runs, starting_port, starting_port, True)
