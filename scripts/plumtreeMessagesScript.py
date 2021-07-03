def plumtree_messages(start_name, n_processes, n_runs, combination, to_print=False):
    sent_gossip = []
    sent_graft = []
    sent_prune = []
    sent_i_have = []
    received_gossip = []
    received_graft = []
    received_prune = []
    received_i_have = []

    for run in range(n_runs):
        sent_gossip.append(0)
        sent_graft.append(0)
        sent_prune.append(0)
        sent_i_have.append(0)
        received_gossip.append(0)
        received_graft.append(0)
        received_prune.append(0)
        received_i_have.append(0)

    for proc in range(n_processes):
        progressBar(proc, n_processes)
        for run in range(n_runs):
            f = open(start_name.format(proc, combination, run+1), "r")
            finalSentGossip = 0
            finalSentGraft = 0
            finalSentPrune = 0
            finalSentIHave = 0
            finalReceivedGossip = 0
            finalReceivedGraft = 0
            finalReceivedPrune = 0
            finalReceivedIHave = 0

            for i in f:
                line = i.split(" ")

                if(i.__contains__('Sent Gossip Msgs:')):
                    finalSentGossip = int(line[3])

                elif(i.__contains__('Sent Graft Msgs:')):
                    finalSentGraft = int(line[3])

                elif(i.__contains__('Sent Prune Msgs:')):
                    finalSentPrune = int(line[3])

                elif(i.__contains__('Sent IHave Msgs:')):
                    finalSentIHave = int(line[3])

                elif(i.__contains__('Received Gossip Msgs:')):
                    finalReceivedGossip = int(line[3])

                elif(i.__contains__('Received Graft Msgs:')):
                    finalReceivedGraft = int(line[3])

                elif(i.__contains__('Received Prune Msgs:')):
                    finalReceivedPrune = int(line[3])

                elif(i.__contains__('Received IHave Msgs:')):
                    finalReceivedIHave = int(line[3])

            sent_gossip[run] += finalSentGossip
            sent_graft[run] += finalSentGraft
            sent_prune[run] += finalSentPrune
            sent_i_have[run] += finalSentIHave
            received_gossip[run] += finalReceivedGossip
            received_graft[run] += finalReceivedGraft
            received_prune[run] += finalReceivedPrune
            received_i_have[run] += finalReceivedIHave

    avg_total_sent = (sum(sent_gossip) + sum(sent_graft) + sum(sent_prune) + sum(sent_i_have)) / n_runs
    percentage_gossip = (sum(sent_gossip) / avg_total_sent) * 100
    percentage_graft = (sum(sent_graft) / avg_total_sent) * 100
    percentage_prune = (sum(sent_prune) / avg_total_sent) * 100
    percentage_i_have = (sum(sent_i_have) / avg_total_sent) * 100

    if(to_print):
        print('PlumTree Messages:')
        print()
        print('Sent Gossip Messages: ', sent_gossip)
        print('Sent Graft Messages: ', sent_graft)
        print('Sent Prune Messages: ', sent_prune)
        print('Sent IHave Messages: ', sent_i_have)
        print()
        print('Received Gossip Messages: ', received_gossip)
        print('Received Graft Messages: ', received_graft)
        print('Received Prune Messages: ', received_prune)
        print('Received IHave Messages: ', received_i_have)
        print()
        print('Percentage Gossip Messages: {:.2f}'.format(percentage_gossip))
        print('Percentage Graft Messages: {:.2f}'.format(percentage_graft))
        print('Percentage Prune Messages: {:.2f}'.format(percentage_prune))
        print('Percentage IHave Messages: {:.2f}'.format(percentage_i_have))
        print()

    return avg_total_sent, percentage_gossip, percentage_graft, percentage_prune, percentage_i_have

def progressBar(current, total, barLength = 20):
    percent = float(current) * 100 / total
    arrow   = '-' * int(percent/100 * barLength - 1) + '>'
    spaces  = ' ' * (barLength - len(arrow))

    print('Progress: [%s%s] %d %%' % (arrow, spaces, percent), end='\r')