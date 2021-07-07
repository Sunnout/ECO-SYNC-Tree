import matplotlib.pyplot as plt
import numpy as np
import sys
import matplotlib.ticker as mtick


def create_bytes_graph(probability, nbytes):
    n = 4
    ind = np.arange(n)
    width = 0.2

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.tick_params(axis='both', which='major', labelsize='large')
    ax.tick_params(axis='both', which='minor', labelsize='large')
    rects1 = ax.bar(ind - width, [nbytes["plumtree"]["50"], nbytes["plumtree"]["100"], nbytes["plumtree"]["150"], nbytes["plumtree"]["200"]], width, color='#009E73', edgecolor='black')
    rects2 = ax.bar(ind, [nbytes["flood"]["50"], nbytes["flood"]["100"], nbytes["flood"]["150"], nbytes["flood"]["200"]], width, color='#E69F00', edgecolor='black')
    rects3 = ax.bar(ind + width, [nbytes["periodicpull"]["50"], nbytes["periodicpull"]["100"], nbytes["periodicpull"]["150"], nbytes["periodicpull"]["200"]], width, color='#9400D3', edgecolor='black')
    ax.set_xticks(ind + width / 2)
    ax.set_xticklabels(('50 nós', '100 nós', '150 nós', '200 nós'), fontsize='large')
    ax.legend((rects1[0], rects2[0], rects3[0]), ("Plumtree Causal", "Flood", "Pull Periódico"), fontsize='large')
    plt.yscale("log")
    plt.ylabel('Número de Bytes Transmitidos', fontsize='x-large')
    plt.savefig('../plots/bytes_prob{}.pdf'.format(probability), format='pdf')


def create_latency_graph(probability, latency):
    n = 4
    ind = np.arange(n)
    width = 0.2

    fig = plt.figure(figsize=(8, 6))
    ax = fig.add_subplot(111)
    ax.tick_params(axis='both', which='major', labelsize='large')
    ax.tick_params(axis='both', which='minor', labelsize='large')
    rects1 = ax.bar(ind - width, [latency["plumtree"]["50"], latency["plumtree"]["100"], latency["plumtree"]["150"], latency["plumtree"]["200"]], width, color='#009E73', edgecolor='black')
    rects2 = ax.bar(ind, [latency["flood"]["50"], latency["flood"]["100"], latency["flood"]["150"], latency["flood"]["200"]], width, color='#E69F00', edgecolor='black')
    rects3 = ax.bar(ind + width, [latency["periodicpull"]["50"], latency["periodicpull"]["100"], latency["periodicpull"]["150"], latency["periodicpull"]["200"]], width, color='#9400D3', edgecolor='black')
    ax.set_xticks(ind + width / 2)
    ax.set_xticklabels(('50 nós', '100 nós', '150 nós', '200 nós'), fontsize='large')
    ax.legend((rects1[0], rects2[0], rects3[0]), ("Plumtree Causal", "Flood", "Pull Periódico"), fontsize='large')
    plt.ylabel('Latência Média de Difusão (ms)', fontsize='x-large')
    plt.savefig('../plots/latency_prob{}.pdf'.format(probability), format='pdf')


def create_percent_dupes_graph(probability, plumtree, flood, pull):
    n = 4
    ind = np.arange(n)
    width = 0.2

    fig = plt.figure()
    ax = fig.add_subplot(111)
    axes = plt.gca()
    axes.set_ylim([None, 70])
    ax.tick_params(axis='both', which='major', labelsize='large')
    ax.tick_params(axis='both', which='minor', labelsize='large')
    rects1 = ax.bar(ind - width, [plumtree["50"], plumtree["100"], plumtree["150"], plumtree["200"]], width, color='#009E73', edgecolor='black')
    rects2 = ax.bar(ind, [flood["50"], flood["100"], flood["150"], flood["200"]], width, color='#E69F00', edgecolor='black')
    rects3 = ax.bar(ind + width, [pull["50"], pull["100"], pull["150"], pull["200"]], width, color='#9400D3', edgecolor='black')
    ax.set_title('Percentagem de Mensagens Duplicadas')
    ax.set_xticks(ind + width / 2)
    ax.set_xticklabels(('50 nós', '100 nós', '150 nós', '200 nós'), fontsize='large')
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.legend((rects1[0], rects2[0], rects3[0]), ("Plumtree Causal", "Flood", "Pull Periódico"), fontsize='large')
    plt.savefig('../plots/percent_dupes_prob{}.pdf'.format(probability), format='pdf')


def create_total_dupes_graph(probability, plumtree, flood, pull):
    n = 4
    ind = np.arange(n)
    width = 0.2

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.tick_params(axis='both', which='major', labelsize='large')
    ax.tick_params(axis='both', which='minor', labelsize='large')
    rects1 = ax.bar(ind - width, [plumtree["50"], plumtree["100"], plumtree["150"], plumtree["200"]], width, color='#009E73', edgecolor='black')
    rects2 = ax.bar(ind, [flood["50"], flood["100"], flood["150"], flood["200"]], width, color='#E69F00', edgecolor='black')
    rects3 = ax.bar(ind + width, [pull["50"], pull["100"], pull["150"], pull["200"]], width, color='#9400D3', edgecolor='black')
    ax.set_xticks(ind + width / 2)
    ax.set_xticklabels(('50 nós', '100 nós', '150 nós', '200 nós'), fontsize='large')
    ax.legend((rects1[0], rects2[0], rects3[0]), ("Plumtree Causal", "Flood", "Pull Periódico"), fontsize='large')
    plt.yscale("log")
    plt.ylabel('Mensagens Duplicadas Recebidas', fontsize='x-large')
    plt.savefig('../plots/total_dupes_prob{}.pdf'.format(probability), format='pdf')


probs = sys.argv[1]
nodes = sys.argv[2]
protocols = sys.argv[3]

probs = probs.split(",")
nodes = nodes.split(",")
protocols = protocols.split(",")

# INIT
results = {}
total_bytes = {}
latency = {}
received_mine_plum = {}
total_received_plum = {}
total_received_flood = {}
total_received_pull = {}
total_dupes_plum = {}
total_dupes_flood = {}
total_dupes_pull = {}
percent_dupes_plum = {}
percent_dupes_flood = {}
percent_dupes_pull = {}

for prob in probs:
    results[prob] = {}
    total_bytes[prob] = {}
    latency[prob] = {}
    received_mine_plum[prob] = {}
    total_received_plum[prob] = {}
    total_received_flood[prob] = {}
    total_received_pull[prob] = {}
    total_dupes_plum[prob] = {}
    total_dupes_flood[prob] = {}
    total_dupes_pull[prob] = {}
    percent_dupes_plum[prob] = {}
    percent_dupes_flood[prob] = {}
    percent_dupes_pull[prob] = {}

    for proto in protocols:
        results[prob][proto] = {}
        total_bytes[prob][proto] = {}
        latency[prob][proto] = {}
        for node in nodes:
            results[prob][proto][node] = 0
            total_bytes[prob][proto][node] = 0
            latency[prob][proto][node] = 0
            received_mine_plum[prob][node] = 0
            total_received_plum[prob][node] = 0
            total_received_flood[prob][node] = 0
            total_received_pull[prob][node] = 0
            total_dupes_plum[prob][node] = 0
            total_dupes_flood[prob][node] = 0
            total_dupes_pull[prob][node] = 0
            percent_dupes_plum[prob][node] = 0
            percent_dupes_flood[prob][node] = 0
            percent_dupes_pull[prob][node] = 0


for prob in probs:
    for node in nodes:
        for proto in protocols:
            # READ FILES
            results[prob][proto][node] = np.genfromtxt(
                "../results/{}nodes_{}_{}prob_runs123.csv".format(node, proto, prob), delimiter=',')

            # TOTAL BYTES
            total_bytes[prob][proto][node] = results[prob][proto][node][3]

            # AVG BCAST LATENCY
            latency[prob][proto][node] = results[prob][proto][node][0]

for prob in probs:
    for node in nodes:
        # MSGS RECEIVED MINE PLUM
        received_mine_plum[prob][node] = results[prob]["plumtree"][node][8] - (results[prob]["plumtree"][node][20] -
                                                                               results[prob]["plumtree"][node][21]) - (
                                                 results[prob]["plumtree"][node][14] -
                                                 results[prob]["plumtree"][node][15])

        # TOTAL MSGS RECEIVED
        total_received_plum[prob][node] = received_mine_plum[prob][node] + results[prob]["plumtree"][node][20] + \
                                          results[prob]["plumtree"][node][14]
        total_received_flood[prob][node] = results[prob]["flood"][node][11] + results[prob]["flood"][node][14]
        total_received_pull[prob][node] = results[prob]["periodicpull"][node][9]

        # TOTAL DUPE MSGS
        total_dupes_plum[prob][node] = results[prob]["plumtree"][node][15] + results[prob]["plumtree"][node][21]
        total_dupes_flood[prob][node] = results[prob]["flood"][node][15] + results[prob]["flood"][node][12]
        total_dupes_plum[prob][node] = results[prob]["periodicpull"][node][10]

        # DUPES %
        percent_dupes_flood[prob][node] = (total_dupes_flood[prob][node] / total_received_flood[prob][node] ) * 100
        percent_dupes_pull[prob][node] = (total_dupes_pull[prob][node] / total_received_pull[prob][node]) * 100
        percent_dupes_plum[prob][node] = (total_dupes_plum[prob][node] / total_received_plum[prob][node]) * 100

# PLOT
for prob in probs:
    create_bytes_graph(prob, total_bytes[prob])
    create_latency_graph(prob, latency[prob])
    create_percent_dupes_graph(prob, percent_dupes_plum[prob], percent_dupes_flood[prob], percent_dupes_pull[prob])
    create_total_dupes_graph(prob, total_dupes_plum[prob], total_dupes_flood[prob], total_dupes_pull[prob])
