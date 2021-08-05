import matplotlib.pyplot as plt
import numpy as np
import sys
import matplotlib.ticker as mtick

patterns = ["//", "\\", "|", "-", "+", "x", "o", "O", ".", "*"]
black_and_white = ['black', 'white', 'white', 'white']
colored = ['#009E73', '#E69F00', '#9400D3', '#56B4E9']
node_labels = ('50 nós', '100 nós', '150 nós', '200 nós')
proto_labels = ("Plumtree Causal", "Inundação Causal", "Sincronização Periódica")


def create_bytes_graph(probability, nbytes, grey_scale):
    n = 4
    ind = np.arange(n)
    width = 0.2

    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.yscale("log")
    ax.tick_params(axis='both', which='major', labelsize='large')
    ax.tick_params(axis='both', which='minor', labelsize='large')
    ax.set_xticks(ind)
    ax.set_xticklabels(node_labels, fontsize='x-large')
    plt.ylabel('Número de Bytes Transmitidos', fontsize='xx-large')

    if grey_scale:
        rects1 = ax.bar(ind - width, [nbytes["plumtree"]["50"], nbytes["plumtree"]["100"], nbytes["plumtree"]["150"],
                                      nbytes["plumtree"]["200"]], width, color=black_and_white[0], edgecolor='black')
        rects2 = ax.bar(ind,
                        [nbytes["flood"]["50"], nbytes["flood"]["100"], nbytes["flood"]["150"], nbytes["flood"]["200"]],
                        width, color=black_and_white[1], edgecolor='black', hatch=patterns[0])
        rects3 = ax.bar(ind + width,
                        [nbytes["periodicpull"]["50"], nbytes["periodicpull"]["100"], nbytes["periodicpull"]["150"],
                         nbytes["periodicpull"]["200"]], width, color=black_and_white[2], edgecolor='black', hatch=patterns[1])
        # rects4 = ax.bar(ind + 2 * width,
        #                 [nbytes["periodicpullsmallertimeout"]["50"], nbytes["periodicpullsmallertimeout"]["100"],
        #                  nbytes["periodicpullsmallertimeout"]["150"], nbytes["periodicpullsmallertimeout"]["200"]],
        #                 width, color=black_and_white[3], edgecolor='black', hatch=patterns[6])
        ax.legend((rects1[0], rects2[0], rects3[0]), proto_labels, fontsize='x-large')
        plt.savefig('../plots/bytes_prob{}_grey.pdf'.format(probability), format='pdf')
    else:
        rects1 = ax.bar(ind - width, [nbytes["plumtree"]["50"], nbytes["plumtree"]["100"], nbytes["plumtree"]["150"],
                                      nbytes["plumtree"]["200"]], width, color=colored[0], edgecolor='black')
        rects2 = ax.bar(ind,
                        [nbytes["flood"]["50"], nbytes["flood"]["100"], nbytes["flood"]["150"], nbytes["flood"]["200"]],
                        width, color=colored[1], edgecolor='black')
        rects3 = ax.bar(ind + width,
                        [nbytes["periodicpull"]["50"], nbytes["periodicpull"]["100"], nbytes["periodicpull"]["150"],
                         nbytes["periodicpull"]["200"]], width, color=colored[2], edgecolor='black')
        # rects4 = ax.bar(ind + 2 * width,
        #                 [nbytes["periodicpullsmallertimeout"]["50"], nbytes["periodicpullsmallertimeout"]["100"],
        #                  nbytes["periodicpullsmallertimeout"]["150"],
        #                  nbytes["periodicpullsmallertimeout"]["200"]], width, color=colored[3], edgecolor='black')
        ax.legend((rects1[0], rects2[0], rects3[0]), proto_labels, fontsize='x-large', framealpha=0.5)
        plt.savefig('../plots/bytes_prob{}.pdf'.format(probability), format='pdf')
    plt.close(fig)


def create_latency_graph(probability, latency, grey_scale):
    n = 4
    ind = np.arange(n)
    width = 0.2

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.set_ylim([None, 45])
    ax.tick_params(axis='both', which='major', labelsize='large')
    ax.tick_params(axis='both', which='minor', labelsize='large')
    ax.set_xticks(ind)
    ax.set_xticklabels(node_labels, fontsize='x-large')
    plt.ylabel('Latência Média de Difusão (s)', fontsize='xx-large')

    if grey_scale:
        rects1 = ax.bar(ind - width, [latency["plumtree"]["50"], latency["plumtree"]["100"], latency["plumtree"]["150"],
                                      latency["plumtree"]["200"]], width, color=black_and_white[0], edgecolor='black')
        rects2 = ax.bar(ind, [latency["flood"]["50"], latency["flood"]["100"], latency["flood"]["150"],
                              latency["flood"]["200"]], width, color=black_and_white[1], edgecolor='black', hatch=patterns[0])
        rects3 = ax.bar(ind + width,
                        [latency["periodicpull"]["50"], latency["periodicpull"]["100"], latency["periodicpull"]["150"],
                         latency["periodicpull"]["200"]], width, color=black_and_white[2], edgecolor='black', hatch=patterns[1])
        # rects4 = ax.bar(ind + 2 * width,
        #                 [latency["periodicpullsmallertimeout"]["50"], latency["periodicpullsmallertimeout"]["100"],
        #                  latency["periodicpullsmallertimeout"]["150"], latency["periodicpullsmallertimeout"]["200"]],
        #                 width, color=black_and_white[3], edgecolor='black', hatch=patterns[6])
        ax.legend((rects1[0], rects2[0], rects3[0]), proto_labels, fontsize='x-large')
        plt.savefig('../plots/latency_prob{}_grey.pdf'.format(probability), format='pdf')
    else:
        rects1 = ax.bar(ind - width, [latency["plumtree"]["50"], latency["plumtree"]["100"], latency["plumtree"]["150"],
                                      latency["plumtree"]["200"]], width, color=colored[0], edgecolor='black')
        rects2 = ax.bar(ind, [latency["flood"]["50"], latency["flood"]["100"], latency["flood"]["150"],
                              latency["flood"]["200"]], width, color=colored[1], edgecolor='black')
        rects3 = ax.bar(ind + width,
                        [latency["periodicpull"]["50"], latency["periodicpull"]["100"], latency["periodicpull"]["150"],
                         latency["periodicpull"]["200"]], width, color=colored[2], edgecolor='black')
        # rects4 = ax.bar(ind + 2 * width,
        #                 [latency["periodicpullsmallertimeout"]["50"], latency["periodicpullsmallertimeout"]["100"],
        #                  latency["periodicpullsmallertimeout"]["150"], latency["periodicpullsmallertimeout"]["200"]],
        #                 width, color='colored[3], edgecolor='black')
        ax.legend((rects1[0], rects2[0], rects3[0]), proto_labels, fontsize='x-large', framealpha=0.5)
        plt.savefig('../plots/latency_prob{}.pdf'.format(probability), format='pdf')
    plt.close(fig)


def create_percent_dupes_graph(probability, plumtree, flood, pull, grey_scale):
    n = 4
    ind = np.arange(n)
    width = 0.2

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.set_ylim([None, 100])
    ax.tick_params(axis='both', which='major', labelsize='x-large')
    ax.tick_params(axis='both', which='minor', labelsize='x-large')
    ax.set_title('Percentagem de Mensagens Duplicadas', fontsize='xx-large')
    ax.set_xticks(ind)
    ax.set_xticklabels(node_labels, fontsize='x-large')
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())

    if grey_scale:
        rects1 = ax.bar(ind - width, [plumtree["50"], plumtree["100"], plumtree["150"], plumtree["200"]], width,
                        color=black_and_white[0], edgecolor='black')
        rects2 = ax.bar(ind, [flood["50"], flood["100"], flood["150"], flood["200"]], width, color=black_and_white[1],
                        edgecolor='black', hatch=patterns[0])
        rects3 = ax.bar(ind + width, [pull["50"], pull["100"], pull["150"], pull["200"]], width, color=black_and_white[2],
                        edgecolor='black', hatch=patterns[1])
        # rects4 = ax.bar(ind + 2 * width,
        #                 [pull_smaller_timeout["50"], pull_smaller_timeout["100"], pull_smaller_timeout["150"],
        #                  pull_smaller_timeout["200"]], width, color=black_and_white[3], edgecolor='black', hatch=patterns[6])
        ax.legend((rects1[0], rects2[0], rects3[0]), proto_labels, fontsize='x-large')
        plt.savefig('../plots/percent_dupes_prob{}_grey.pdf'.format(probability), format='pdf')
    else:
        rects1 = ax.bar(ind - width, [plumtree["50"], plumtree["100"], plumtree["150"], plumtree["200"]], width,
                        color=colored[0], edgecolor='black')
        rects2 = ax.bar(ind, [flood["50"], flood["100"], flood["150"], flood["200"]], width, color=colored[1],
                        edgecolor='black')
        rects3 = ax.bar(ind + width, [pull["50"], pull["100"], pull["150"], pull["200"]], width, color=colored[2],
                        edgecolor='black')
        # rects4 = ax.bar(ind + 2 * width,
        #                 [pull_smaller_timeout["50"], pull_smaller_timeout["100"], pull_smaller_timeout["150"],
        #                  pull_smaller_timeout["200"]], width, color=colored[3], edgecolor='black')
        ax.legend((rects1[0], rects2[0], rects3[0]), proto_labels, fontsize='x-large')
        plt.savefig('../plots/percent_dupes_prob{}.pdf'.format(probability), format='pdf')
    plt.close(fig)


def create_total_dupes_graph(probability, plumtree, flood, pull, grey_scale):
    n = 4
    ind = np.arange(n)
    width = 0.2

    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.yscale("log")
    ax.tick_params(axis='both', which='major', labelsize='x-large')
    ax.tick_params(axis='both', which='minor', labelsize='x-large')
    ax.set_xticks(ind)
    ax.set_xticklabels(node_labels, fontsize='x-large')
    plt.ylabel('Mensagens Duplicadas Recebidas', fontsize='xx-large')

    if grey_scale:
        rects1 = ax.bar(ind - width, [plumtree["50"], plumtree["100"], plumtree["150"], plumtree["200"]], width,
                        color=black_and_white[0], edgecolor='black')
        rects2 = ax.bar(ind, [flood["50"], flood["100"], flood["150"], flood["200"]], width, color=black_and_white[1],
                        edgecolor='black', hatch=patterns[0])
        rects3 = ax.bar(ind + width, [pull["50"], pull["100"], pull["150"], pull["200"]], width, color=black_and_white[2],
                        edgecolor='black', hatch=patterns[1])
        # rects4 = ax.bar(ind + 2 * width,
        #                 [pull_smaller_timeout["50"], pull_smaller_timeout["100"], pull_smaller_timeout["150"],
        #                  pull_smaller_timeout["200"]], width, color=black_and_white[3], edgecolor='black', hatch=patterns[6])
        ax.legend((rects1[0], rects2[0], rects3[0]), proto_labels, fontsize='x-large')
        plt.savefig('../plots/total_dupes_prob{}_grey.pdf'.format(probability), format='pdf')
    else:
        rects1 = ax.bar(ind - width, [plumtree["50"], plumtree["100"], plumtree["150"], plumtree["200"]], width,
                        color=colored[0], edgecolor='black')
        rects2 = ax.bar(ind, [flood["50"], flood["100"], flood["150"], flood["200"]], width, color=colored[1],
                        edgecolor='black')
        rects3 = ax.bar(ind + width, [pull["50"], pull["100"], pull["150"], pull["200"]], width, color=colored[2],
                        edgecolor='black')
        # rects4 = ax.bar(ind + 2 * width,
        #                 [pull_smaller_timeout["50"], pull_smaller_timeout["100"], pull_smaller_timeout["150"],
        #                  pull_smaller_timeout["200"]], width, color=colored[3], edgecolor='black')
        ax.legend((rects1[0], rects2[0], rects3[0]), proto_labels, fontsize='x-large', framealpha=0.5, loc=2)
        plt.savefig('../plots/total_dupes_prob{}.pdf'.format(probability), format='pdf')
    plt.close(fig)


def create_dupes_per_interval(probability, protocol, nodes, interval, ys, grey_scale):
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(111)
    colors = [0, 0, 0]
    times = []
    times.append(interval * 60)
    for t in range(1, 150):
        times.append(times[t - 1] + (interval * 60))

    if grey_scale:
        colors[:] = 'black'
    else:
        colors[0] = '#009E73'
        colors[1] = '#E69F00'
        colors[2] = '#9400D3'

    i = 0
    for y in ys:
        y_line = y.split(",")
        y_line[-1] = y_line[-1].rstrip()
        while len(y_line) < 150:
            if y_line[0] == '':
                y_line[0] = 0
            y_line.append(0)
        y_line = list(map(int, y_line))
        plt.plot(times, y_line, label=f"{protocol.title()} - run {i + 1}", markersize=10, marker=".", color=colors[i])
        i = i + 1
    plt.legend()
    ax.tick_params(axis='both', labelsize='x-large')
    ax.legend(fontsize='x-large')
    plt.title(f"{protocol.title()} com {nodes} nós e probabilidade {probability}", fontsize='xx-large')
    plt.ylabel('Mensagens Duplicadas Recebidas', fontsize='xx-large')
    plt.xlabel('Tempo da experiência (s)', fontsize='xx-large')
    plt.savefig('../plots/dupes_per_interval_{}_{}_{}.pdf'.format(node, protocol, probability), format='pdf')
    plt.close(fig)


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
total_received_plum = {}
total_received_flood = {}
total_received_pull = {}
total_received_pull_smaller_timeout = {}
total_dupes_plum = {}
total_dupes_flood = {}
total_dupes_pull = {}
total_dupes_pull_smaller_timeout = {}
percent_dupes_plum = {}
percent_dupes_flood = {}
percent_dupes_pull = {}
percent_dupes_pull_smaller_timeout = {}

for prob in probs:
    results[prob] = {}
    total_bytes[prob] = {}
    latency[prob] = {}
    total_received_plum[prob] = {}
    total_received_flood[prob] = {}
    total_received_pull[prob] = {}
    total_received_pull_smaller_timeout[prob] = {}
    total_dupes_plum[prob] = {}
    total_dupes_flood[prob] = {}
    total_dupes_pull[prob] = {}
    total_dupes_pull_smaller_timeout[prob] = {}
    percent_dupes_plum[prob] = {}
    percent_dupes_flood[prob] = {}
    percent_dupes_pull[prob] = {}
    percent_dupes_pull_smaller_timeout[prob] = {}

    for proto in protocols:
        results[prob][proto] = {}
        total_bytes[prob][proto] = {}
        latency[prob][proto] = {}
        for node in nodes:
            results[prob][proto][node] = 0
            total_bytes[prob][proto][node] = 0
            latency[prob][proto][node] = 0
            total_received_plum[prob][node] = 0
            total_received_flood[prob][node] = 0
            total_received_pull[prob][node] = 0
            total_received_pull_smaller_timeout[prob][node] = 0
            total_dupes_plum[prob][node] = 0
            total_dupes_flood[prob][node] = 0
            total_dupes_pull[prob][node] = 0
            total_dupes_pull_smaller_timeout[prob][node] = 0
            percent_dupes_plum[prob][node] = 0
            percent_dupes_flood[prob][node] = 0
            percent_dupes_pull[prob][node] = 0
            percent_dupes_pull_smaller_timeout[prob][node] = 0

for prob in probs:
    for node in nodes:
        for proto in protocols:
            # READ FILES
            results[prob][proto][node] = np.genfromtxt(
                "../newResults/{}nodes_{}_{}prob_runs123.csv".format(node, proto, prob), delimiter=',')

            # TOTAL BYTES
            total_bytes[prob][proto][node] = results[prob][proto][node][3]

            # AVG BCAST LATENCY
            latency[prob][proto][node] = results[prob][proto][node][0] / 1000

for prob in probs:
    for node in nodes:
        if "plumtree" in protocols:
            # PLUM
            total_received_plum[prob][node] = results[prob]["plumtree"][node][20] + results[prob]["plumtree"][node][14]
            total_dupes_plum[prob][node] = results[prob]["plumtree"][node][15] + results[prob]["plumtree"][node][21]
            percent_dupes_plum[prob][node] = (total_dupes_plum[prob][node] / total_received_plum[prob][node]) * 100

        if "flood" in protocols:
            # FLOOD
            total_received_flood[prob][node] = results[prob]["flood"][node][11] + results[prob]["flood"][node][14]
            total_dupes_flood[prob][node] = results[prob]["flood"][node][15] + results[prob]["flood"][node][12]
            percent_dupes_flood[prob][node] = (total_dupes_flood[prob][node] / total_received_flood[prob][node]) * 100

        if "periodicpull" in protocols:
            # PULL
            total_received_pull[prob][node] = results[prob]["periodicpull"][node][9]
            total_dupes_pull[prob][node] = results[prob]["periodicpull"][node][10]
            percent_dupes_pull[prob][node] = (total_dupes_pull[prob][node] / total_received_pull[prob][node]) * 100

        if "periodicpullsmallertimeout" in protocols:
            # PULL SMALLER TIMEOUT (1s)
            total_received_pull_smaller_timeout[prob][node] = results[prob]["periodicpullsmallertimeout"][node][9]
            total_dupes_pull_smaller_timeout[prob][node] = results[prob]["periodicpullsmallertimeout"][node][10]
            percent_dupes_pull_smaller_timeout[prob][node] = (total_dupes_pull_smaller_timeout[prob][node] /
                                                              total_received_pull_smaller_timeout[prob][node]) * 100

# PLOT
for prob in probs:
    create_bytes_graph(prob, total_bytes[prob], False)
    create_latency_graph(prob, latency[prob], False)
    create_percent_dupes_graph(prob, percent_dupes_plum[prob], percent_dupes_flood[prob], percent_dupes_pull[prob], False)
    create_total_dupes_graph(prob, total_dupes_plum[prob], total_dupes_flood[prob], total_dupes_pull[prob], False)

    create_bytes_graph(prob, total_bytes[prob], True)
    create_latency_graph(prob, latency[prob], True)
    create_percent_dupes_graph(prob, percent_dupes_plum[prob], percent_dupes_flood[prob], percent_dupes_pull[prob], True)
    create_total_dupes_graph(prob, total_dupes_plum[prob], total_dupes_flood[prob], total_dupes_pull[prob], True)

# DUPES PER INTERVAL
for proto in protocols:
    for prob in probs:
        for node in nodes:
            ys = open(f"../newResults/dupes_by_interval_{node}nodes_{proto}_{prob}_3runs.csv", "r")
            create_dupes_per_interval(prob, proto, node, 0.1, ys, False)
