def messages_bytes(start_name, n_processes, n_runs, combination, to_print=False):
    messages_transmitted = []
    messages_received = []
    bytes_transmitted = []
    bytes_received = []

    for run in range(n_runs):
        messages_transmitted.append(0)
        messages_received.append(0)
        bytes_transmitted.append(0)
        bytes_received.append(0)

    for proc in range(n_processes):
        progressBar(proc, n_processes)
        for run in range(n_runs):
            f = open(start_name.format(proc, combination, run+1), "r")
            final_bytes_transmitted = 0
            final_bytes_received = 0
            final_messages_transmitted = 0
            final_messages_received = 0

            for i in f:
                line = i.split(" ")

                if(line[0].__contains__('BytesSent')):
                    final_bytes_transmitted = int(line[2])

                elif(line[0].__contains__('MessagesSent')):
                    final_messages_transmitted = int(line[2])

                elif(line[0].__contains__('BytesReceived')):
                    final_bytes_received = int(line[2])

                elif(line[0].__contains__('MessagesReceived')):
                    final_messages_received = int(line[2])

            messages_transmitted[run] += final_messages_transmitted
            messages_received[run] += final_messages_received
            bytes_transmitted[run] += final_bytes_transmitted
            bytes_received[run] += final_bytes_received

    print("Progress: [------------------->] 100%", end='\n')
    total_percentages_lost = 0

    for run in range(n_runs):
        total_percentages_lost += (((messages_transmitted[run] - messages_received[run]) / messages_transmitted[run]) * 100)

    avg_percentage_lost = total_percentages_lost / n_runs
    avg_mess_trans = sum(messages_transmitted) / n_runs
    avg_mess_rec = sum(messages_received) / n_runs
    avg_bytes_trans = sum(bytes_transmitted) / n_runs
    avg_bytes_rec = sum(bytes_received) / n_runs

    if(to_print):
        print('Total Transmitted and Received Content:')
        print('Avg Messages Transmitted: ', avg_mess_trans)
        print('Avg Messages Received: ', avg_mess_rec)
        print('Avg Bytes Transmitted: ', avg_bytes_trans)
        print('Avg Bytes Received: ', avg_bytes_rec)
        print('Avg Percentage of messages lost: {:.2f}%'.format(avg_percentage_lost))
        print()

    return avg_mess_trans, avg_mess_rec, avg_bytes_trans, avg_bytes_rec

def progressBar(current, total, barLength = 20):
    percent = float(current) * 100 / total
    arrow   = '-' * int(percent/100 * barLength - 1) + '>'
    spaces  = ' ' * (barLength - len(arrow))

    print('Progress: [%s%s] %d %%' % (arrow, spaces, percent), end='\r')

#start_name = "./results/results-Alexandres-MBP.lan-{}.txt"
#n_processes = 5
#starting_port = 5000

#messages_bytes(start_name, n_processes, starting_port)