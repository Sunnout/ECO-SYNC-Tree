import matplotlib.pyplot as plt
import numpy as np


def create_bytes_graph(probability, plumtree, flood, pull):
    n = 4
    ind = np.arange(n)
    width = 0.2

    fig = plt.figure()
    ax = fig.add_subplot(111)
    rects1 = ax.bar(ind - width, plumtree, width, color='lightblue', edgecolor='black')
    rects2 = ax.bar(ind, flood, width, color='steelblue', edgecolor='black')
    rects3 = ax.bar(ind + width, pull, width, color='lightgray', edgecolor='black')
    ax.set_title('Cost of Communication')
    ax.set_xticks(ind + width / 2)
    ax.set_xticklabels(('50 nodes', '100 nodes', '152 nodes', '200 nodes'), fontsize='x-small')
    ax.legend((rects1[0], rects2[0], rects3[0]), ("Causal Plumtree", "Flood", "Periodic Pull"), fontsize='x-small')
    plt.yscale("log")
    plt.show
    plt.savefig('bytes_prob{}.pdf'.format(probability), format='pdf')


def create_latency_graph(probability, plumtree, flood, pull):
    n = 4
    ind = np.arange(n)
    width = 0.15

    fig = plt.figure()
    ax = fig.add_subplot(111)
    rects1 = ax.bar(ind - width, plumtree, width, color='lightblue', edgecolor='black')
    rects2 = ax.bar(ind, flood, width, color='steelblue', edgecolor='black')
    rects3 = ax.bar(ind + width, pull, width, color='lightgray', edgecolor='black')
    ax.set_title('Average Broadcast Latency (ms)')
    ax.set_xticks(ind + width / 2)
    ax.set_xticklabels(('50 nodes', '100 nodes', '152 nodes', '200 nodes'), fontsize='x-small')
    ax.legend((rects1[0], rects2[0], rects3[0]), ("Causal Plumtree", "Flood", "Periodic Pull"), fontsize='x-small')
    plt.savefig('latency_prob{}.pdf'.format(probability), format='pdf')


def create_dupes_graph(probability, plumtree, flood, pull):
    n = 4
    ind = np.arange(n)
    width = 0.15

    fig = plt.figure()
    ax = fig.add_subplot(111)
    rects1 = ax.bar(ind - width, plumtree, width, color='lightblue', edgecolor='black')
    rects2 = ax.bar(ind, flood, width, color='steelblue', edgecolor='black')
    rects3 = ax.bar(ind + width, pull, width, color='lightgray', edgecolor='black')
    ax.set_title('Percentage of Duplicates')
    ax.set_xticks(ind + width / 2)
    ax.set_xticklabels(('50 nodes', '100 nodes', '152 nodes', '200 nodes'), fontsize='x-small')
    ax.legend((rects1[0], rects2[0], rects3[0]), ("Causal Plumtree", "Flood", "Periodic Pull"), fontsize='x-small')
    plt.savefig('dupes_prob{}.pdf'.format(probability), format='pdf')


#READ FILES
nodes50_plumtree_prob1 = np.genfromtxt("../results/50nodes_plumtree_1prob_runs123.csv", delimiter=',')
nodes50_plumtree_prob5 = np.genfromtxt("../results/50nodes_plumtree_0.5prob_runs123.csv", delimiter=',')
nodes50_plumtree_prob2 = np.genfromtxt("../results/50nodes_plumtree_0.2prob_runs123.csv", delimiter=',')
nodes50_flood_prob1 = np.genfromtxt("../results/50nodes_flood_1prob_runs123.csv", delimiter=',')
nodes50_flood_prob5 = np.genfromtxt("../results/50nodes_flood_0.5prob_runs123.csv", delimiter=',')
nodes50_flood_prob2 = np.genfromtxt("../results/50nodes_flood_0.2prob_runs123.csv", delimiter=',')
nodes50_pull_prob1 = np.genfromtxt("../results/50nodes_periodicpull_1prob_runs123.csv", delimiter=',')
nodes50_pull_prob5 = np.genfromtxt("../results/50nodes_periodicpull_0.5prob_runs123.csv", delimiter=',')
nodes50_pull_prob2 = np.genfromtxt("../results/50nodes_periodicpull_0.2prob_runs123.csv", delimiter=',')

nodes100_plumtree_prob1 = np.genfromtxt("../results/100nodes_plumtree_1prob_runs123.csv", delimiter=',')
nodes100_plumtree_prob5 = np.genfromtxt("../results/100nodes_plumtree_0.5prob_runs123.csv", delimiter=',')
nodes100_plumtree_prob2 = np.genfromtxt("../results/100nodes_plumtree_0.2prob_runs123.csv", delimiter=',')
nodes100_flood_prob1 = np.genfromtxt("../results/100nodes_flood_1prob_runs123.csv", delimiter=',')
nodes100_flood_prob5 = np.genfromtxt("../results/100nodes_flood_0.5prob_runs123.csv", delimiter=',')
nodes100_flood_prob2 = np.genfromtxt("../results/100nodes_flood_0.2prob_runs123.csv", delimiter=',')
nodes100_pull_prob1 = np.genfromtxt("../results/100nodes_periodicpull_1prob_runs123.csv", delimiter=',')
nodes100_pull_prob5 = np.genfromtxt("../results/100nodes_periodicpull_0.5prob_runs123.csv", delimiter=',')
nodes100_pull_prob2 = np.genfromtxt("../results/100nodes_periodicpull_0.2prob_runs123.csv", delimiter=',')

nodes152_plumtree_prob1 = np.genfromtxt("../results/152nodes_plumtree_1prob_runs123.csv", delimiter=',')
nodes152_plumtree_prob5 = np.genfromtxt("../results/152nodes_plumtree_0.5prob_runs123.csv", delimiter=',')
nodes152_plumtree_prob2 = np.genfromtxt("../results/152nodes_plumtree_0.2prob_runs123.csv", delimiter=',')
nodes152_flood_prob1 = np.genfromtxt("../results/152nodes_flood_1prob_runs123.csv", delimiter=',')
nodes152_flood_prob5 = np.genfromtxt("../results/152nodes_flood_0.5prob_runs123.csv", delimiter=',')
nodes152_flood_prob2 = np.genfromtxt("../results/152nodes_flood_0.2prob_runs123.csv", delimiter=',')
nodes152_pull_prob1 = np.genfromtxt("../results/152nodes_periodicpull_1prob_runs123.csv", delimiter=',')
nodes152_pull_prob5 = np.genfromtxt("../results/152nodes_periodicpull_0.5prob_runs123.csv", delimiter=',')
nodes152_pull_prob2 = np.genfromtxt("../results/152nodes_periodicpull_0.2prob_runs123.csv", delimiter=',')

nodes200_plumtree_prob1 = np.genfromtxt("../results/200nodes_plumtree_1prob_runs123.csv", delimiter=',')
nodes200_plumtree_prob5 = np.genfromtxt("../results/200nodes_plumtree_0.5prob_runs123.csv", delimiter=',')
nodes200_plumtree_prob2 = np.genfromtxt("../results/200nodes_plumtree_0.2prob_runs123.csv", delimiter=',')
nodes200_flood_prob1 = np.genfromtxt("../results/200nodes_flood_1prob_runs123.csv", delimiter=',')
nodes200_flood_prob5 = np.genfromtxt("../results/200nodes_flood_0.5prob_runs123.csv", delimiter=',')
nodes200_flood_prob2 = np.genfromtxt("../results/200nodes_flood_0.2prob_runs123.csv", delimiter=',')
nodes200_pull_prob1 = np.genfromtxt("../results/200nodes_periodicpull_1prob_runs123.csv", delimiter=',')
nodes200_pull_prob5 = np.genfromtxt("../results/200nodes_periodicpull_0.5prob_runs123.csv", delimiter=',')
nodes200_pull_prob2 = np.genfromtxt("../results/200nodes_periodicpull_0.2prob_runs123.csv", delimiter=',')


#BYTES
plumtree_bytes_2 = [nodes50_plumtree_prob2[3], nodes100_plumtree_prob2[3], nodes152_plumtree_prob2[3], nodes200_plumtree_prob2[3]]
flood_bytes_2 = [nodes50_flood_prob2[3], nodes100_flood_prob2[3], nodes152_flood_prob2[3], nodes200_flood_prob2[3]]
pull_bytes_2 = [nodes50_pull_prob2[3], nodes100_pull_prob2[3], nodes152_pull_prob2[3], nodes200_pull_prob2[3]]
create_bytes_graph(0.2, plumtree_bytes_2, flood_bytes_2, pull_bytes_2)

plumtree_bytes_5 = [nodes50_plumtree_prob5[3], nodes100_plumtree_prob5[3], nodes152_plumtree_prob5[3], nodes200_plumtree_prob5[3]]
flood_bytes_5 = [nodes50_flood_prob5[3], nodes100_flood_prob5[3], nodes152_flood_prob5[3], nodes200_flood_prob5[3]]
pull_bytes_5 = [nodes50_pull_prob5[3], nodes100_pull_prob5[3], nodes152_pull_prob5[3], nodes200_pull_prob5[3]]
create_bytes_graph(0.5, plumtree_bytes_5, flood_bytes_5, pull_bytes_5)

plumtree_bytes_1 = [nodes50_plumtree_prob1[3], nodes100_plumtree_prob1[3], nodes152_plumtree_prob1[3], nodes200_plumtree_prob1[3]]
flood_bytes_1 = [nodes50_flood_prob1[3], nodes100_flood_prob1[3], nodes152_flood_prob1[3], nodes200_flood_prob1[3]]
pull_bytes_1 = [nodes50_pull_prob1[3], nodes100_pull_prob1[3], nodes152_pull_prob1[3], nodes200_pull_prob1[3]]
create_bytes_graph(1, plumtree_bytes_1, flood_bytes_1, pull_bytes_1)

#LATENCY
plumtree_lat_2 = [nodes50_plumtree_prob2[0], nodes100_plumtree_prob2[0], nodes152_plumtree_prob2[0], nodes200_plumtree_prob2[0]]
flood_lat_2 = [nodes50_flood_prob2[0], nodes100_flood_prob2[0], nodes152_flood_prob2[0], nodes200_flood_prob2[0]]
pull_lat_2 = [nodes50_pull_prob2[0], nodes100_pull_prob2[0], nodes152_pull_prob2[0], nodes200_pull_prob2[0]]
create_latency_graph(0.2, plumtree_lat_2, flood_lat_2, pull_lat_2)

plumtree_lat_5 = [nodes50_plumtree_prob5[0], nodes100_plumtree_prob5[0], nodes152_plumtree_prob5[0], nodes200_plumtree_prob5[0]]
flood_lat_5 = [nodes50_flood_prob5[0], nodes100_flood_prob5[0], nodes152_flood_prob5[0], nodes200_flood_prob5[0]]
pull_lat_5 = [nodes50_pull_prob5[0], nodes100_pull_prob5[0], nodes152_pull_prob5[0], nodes200_pull_prob5[0]]
create_latency_graph(0.5, plumtree_lat_5, flood_lat_5, pull_lat_5)

plumtree_lat_1 = [nodes50_plumtree_prob1[0], nodes100_plumtree_prob1[0], nodes152_plumtree_prob1[0], nodes200_plumtree_prob1[0]]
flood_lat_1 = [nodes50_flood_prob1[0], nodes100_flood_prob1[0], nodes152_flood_prob1[0], nodes200_flood_prob1[0]]
pull_lat_1 = [nodes50_pull_prob1[0], nodes100_pull_prob1[0], nodes152_pull_prob1[0], nodes200_pull_prob1[0]]
create_latency_graph(1, plumtree_lat_1, flood_lat_1, pull_lat_1)

#RECEIVED MINE PLUM
nodes50_received_plumtree_2_mine = nodes50_plumtree_prob2[8] - (nodes50_plumtree_prob2[20] - nodes50_plumtree_prob2[21]) - (nodes50_plumtree_prob2[14] - nodes50_plumtree_prob2[15])
nodes100_received_plumtree_2_mine = nodes100_plumtree_prob2[8] - (nodes100_plumtree_prob2[20] - nodes100_plumtree_prob2[21]) - (nodes100_plumtree_prob2[14] - nodes100_plumtree_prob2[15])
nodes152_received_plumtree_2_mine = nodes152_plumtree_prob2[8] - (nodes152_plumtree_prob2[20] - nodes152_plumtree_prob2[21]) - (nodes152_plumtree_prob2[14] - nodes152_plumtree_prob2[15])
nodes200_received_plumtree_2_mine = nodes200_plumtree_prob2[8] - (nodes200_plumtree_prob2[20] - nodes200_plumtree_prob2[21]) - (nodes200_plumtree_prob2[14] - nodes200_plumtree_prob2[15])

nodes50_received_plumtree_5_mine = nodes50_plumtree_prob5[8] - (nodes50_plumtree_prob5[20] - nodes50_plumtree_prob5[21]) - (nodes50_plumtree_prob5[14] - nodes50_plumtree_prob5[15])
nodes100_received_plumtree_5_mine = nodes100_plumtree_prob5[8] - (nodes100_plumtree_prob5[20] - nodes100_plumtree_prob5[21]) - (nodes100_plumtree_prob5[14] - nodes100_plumtree_prob5[15])
nodes152_received_plumtree_5_mine = nodes152_plumtree_prob5[8] - (nodes152_plumtree_prob5[20] - nodes152_plumtree_prob5[21]) - (nodes152_plumtree_prob5[14] - nodes152_plumtree_prob5[15])
nodes200_received_plumtree_5_mine = nodes200_plumtree_prob5[8] - (nodes200_plumtree_prob5[20] - nodes200_plumtree_prob5[21]) - (nodes200_plumtree_prob5[14] - nodes200_plumtree_prob5[15])

nodes50_received_plumtree_1_mine = nodes50_plumtree_prob1[8] - (nodes50_plumtree_prob1[20] - nodes50_plumtree_prob1[21]) - (nodes50_plumtree_prob1[14] - nodes50_plumtree_prob1[15])
nodes100_received_plumtree_1_mine = nodes100_plumtree_prob1[8] - (nodes100_plumtree_prob1[20] - nodes100_plumtree_prob1[21]) - (nodes100_plumtree_prob1[14] - nodes100_plumtree_prob1[15])
nodes152_received_plumtree_1_mine = nodes152_plumtree_prob1[8] - (nodes152_plumtree_prob1[20] - nodes152_plumtree_prob1[21]) - (nodes152_plumtree_prob1[14] - nodes152_plumtree_prob1[15])
nodes200_received_plumtree_1_mine = nodes200_plumtree_prob1[8] - (nodes200_plumtree_prob1[20] - nodes200_plumtree_prob1[21]) - (nodes200_plumtree_prob1[14] - nodes200_plumtree_prob1[15])

#TOTAL RECEIVED
nodes50_received_plumtree_2 = nodes50_received_plumtree_2_mine + nodes50_plumtree_prob2[20] + nodes50_plumtree_prob2[14]
nodes100_received_plumtree_2 =  nodes100_received_plumtree_2_mine + nodes100_plumtree_prob2[20] + nodes100_plumtree_prob2[14]
nodes152_received_plumtree_2 = nodes152_received_plumtree_2_mine + nodes152_plumtree_prob2[20] + nodes152_plumtree_prob2[14]
nodes200_received_plumtree_2 = nodes200_received_plumtree_2_mine + nodes200_plumtree_prob2[20] + nodes200_plumtree_prob2[14]

nodes50_received_plumtree_5 = nodes50_received_plumtree_5_mine + nodes50_plumtree_prob5[20] + nodes50_plumtree_prob5[14]
nodes100_received_plumtree_5 =  nodes100_received_plumtree_5_mine + nodes100_plumtree_prob5[20] + nodes100_plumtree_prob5[14]
nodes152_received_plumtree_5 = nodes152_received_plumtree_5_mine + nodes152_plumtree_prob5[20] + nodes152_plumtree_prob5[14]
nodes200_received_plumtree_5 = nodes200_received_plumtree_5_mine + nodes200_plumtree_prob5[20] + nodes200_plumtree_prob5[14]

nodes50_received_plumtree_1 = nodes50_received_plumtree_1_mine + nodes50_plumtree_prob1[20] + nodes50_plumtree_prob1[14]
nodes100_received_plumtree_1 =  nodes100_received_plumtree_1_mine + nodes100_plumtree_prob1[20] + nodes100_plumtree_prob1[14]
nodes152_received_plumtree_1 = nodes152_received_plumtree_1_mine + nodes152_plumtree_prob1[20] + nodes152_plumtree_prob1[14]
nodes200_received_plumtree_1 = nodes200_received_plumtree_1_mine + nodes200_plumtree_prob1[20] + nodes200_plumtree_prob1[14]


nodes50_received_flood_2 = nodes50_flood_prob2[11] + nodes50_flood_prob2[14]
nodes100_received_flood_2 = nodes100_flood_prob2[11] + nodes100_flood_prob2[14]
nodes152_received_flood_2 = nodes152_flood_prob2[11] + nodes152_flood_prob2[14]
nodes200_received_flood_2 = nodes200_flood_prob2[11] + nodes200_flood_prob2[14]

nodes50_received_flood_5 = nodes50_flood_prob5[11] + nodes50_flood_prob5[14]
nodes100_received_flood_5 = nodes100_flood_prob5[11] + nodes100_flood_prob5[14]
nodes152_received_flood_5 = nodes152_flood_prob5[11] + nodes152_flood_prob5[14]
nodes200_received_flood_5 = nodes200_flood_prob5[11] + nodes200_flood_prob5[14]

nodes50_received_flood_1 = nodes50_flood_prob1[11] + nodes50_flood_prob1[14]
nodes100_received_flood_1 = nodes100_flood_prob1[11] + nodes100_flood_prob1[14]
nodes152_received_flood_1 = nodes152_flood_prob1[11] + nodes152_flood_prob1[14]
nodes200_received_flood_1 = nodes200_flood_prob1[11] + nodes200_flood_prob1[14]

#DUPES
plum_dup_50_1 = ((nodes50_plumtree_prob1[15] + nodes50_plumtree_prob1[21]) / nodes50_received_plumtree_1) * 100
plum_dup_50_2 = ((nodes50_plumtree_prob2[15] + nodes50_plumtree_prob2[21]) / nodes50_received_plumtree_2) * 100
plum_dup_50_5 = ((nodes50_plumtree_prob5[15] + nodes50_plumtree_prob5[21]) / nodes50_received_plumtree_5) * 100

plum_dup_100_1 = ((nodes100_plumtree_prob1[15] + nodes100_plumtree_prob1[21]) / nodes100_received_plumtree_1) * 100
plum_dup_100_2 = ((nodes100_plumtree_prob2[15] + nodes100_plumtree_prob2[21]) / nodes100_received_plumtree_2) * 100
plum_dup_100_5 = ((nodes100_plumtree_prob5[15] + nodes100_plumtree_prob5[21]) / nodes100_received_plumtree_5) * 100

plum_dup_152_1 = ((nodes152_plumtree_prob1[15] + nodes152_plumtree_prob1[21]) / nodes152_received_plumtree_1) * 100
plum_dup_152_2 = ((nodes152_plumtree_prob2[15] + nodes152_plumtree_prob2[21]) / nodes152_received_plumtree_2) * 100
plum_dup_152_5 = ((nodes152_plumtree_prob5[15] + nodes152_plumtree_prob5[21]) / nodes152_received_plumtree_5) * 100

plum_dup_200_1 = ((nodes200_plumtree_prob1[15] + nodes200_plumtree_prob1[21]) / nodes200_received_plumtree_1) * 100
plum_dup_200_2 = ((nodes200_plumtree_prob2[15] + nodes200_plumtree_prob2[21]) / nodes200_received_plumtree_2) * 100
plum_dup_200_5 = ((nodes200_plumtree_prob5[15] + nodes200_plumtree_prob5[21]) / nodes200_received_plumtree_5) * 100

flood_dup_50_1 = ((nodes50_flood_prob1[15] + nodes50_flood_prob1[12]) / nodes50_received_flood_1) * 100
flood_dup_50_2 = ((nodes50_flood_prob2[15] + nodes50_flood_prob2[12]) / nodes50_received_flood_2) * 100
flood_dup_50_5 = ((nodes50_flood_prob5[15] + nodes50_flood_prob5[12]) / nodes50_received_flood_5) * 100

flood_dup_100_1 = ((nodes100_flood_prob1[15] + nodes100_flood_prob1[12]) / nodes100_received_flood_1) * 100
flood_dup_100_2 = ((nodes100_flood_prob2[15] + nodes100_flood_prob2[12]) / nodes100_received_flood_2) * 100
flood_dup_100_5 = ((nodes100_flood_prob5[15] + nodes100_flood_prob5[12]) / nodes100_received_flood_5) * 100

flood_dup_152_1 = ((nodes152_flood_prob1[15] + nodes152_flood_prob1[12]) / nodes152_received_flood_1) * 100
flood_dup_152_2 = ((nodes152_flood_prob2[15] + nodes152_flood_prob2[12]) / nodes152_received_flood_2) * 100
flood_dup_152_5 = ((nodes152_flood_prob5[15] + nodes152_flood_prob5[12]) / nodes152_received_flood_5) * 100

flood_dup_200_1 = ((nodes200_flood_prob1[15] + nodes200_flood_prob1[12]) / nodes200_received_flood_1) * 100
flood_dup_200_2 = ((nodes200_flood_prob2[15] + nodes200_flood_prob2[12]) / nodes200_received_flood_2) * 100
flood_dup_200_5 = ((nodes200_flood_prob5[15] + nodes200_flood_prob5[12]) / nodes200_received_flood_5) * 100

pull_dup_50_1 = (nodes50_pull_prob1[10] / nodes50_pull_prob1[9]) * 100
pull_dup_50_2 = (nodes50_pull_prob2[10] / nodes50_pull_prob2[9]) * 100
pull_dup_50_5 = (nodes50_pull_prob5[10] / nodes50_pull_prob5[9]) * 100

pull_dup_100_1 = (nodes100_pull_prob1[10] / nodes100_pull_prob1[9]) * 100
pull_dup_100_2 = (nodes100_pull_prob2[10] / nodes100_pull_prob2[9]) * 100
pull_dup_100_5 = (nodes100_pull_prob5[10] / nodes100_pull_prob5[9]) * 100

pull_dup_152_1 = (nodes152_pull_prob1[10] / nodes152_pull_prob1[9]) * 100
pull_dup_152_2 = (nodes152_pull_prob2[10] / nodes152_pull_prob2[9]) * 100
pull_dup_152_5 = (nodes152_pull_prob5[10] / nodes152_pull_prob5[9]) * 100

pull_dup_200_1 = (nodes200_pull_prob1[10] / nodes200_pull_prob1[9]) * 100
pull_dup_200_2 = (nodes200_pull_prob2[10] / nodes200_pull_prob2[9]) * 100
pull_dup_200_5 = (nodes200_pull_prob5[10] / nodes200_pull_prob5[9]) * 100


create_dupes_graph(1, [plum_dup_50_1, plum_dup_100_1, plum_dup_152_1, plum_dup_200_1],
                   [flood_dup_50_1, flood_dup_100_1, flood_dup_152_1, flood_dup_200_1], [pull_dup_50_1, pull_dup_100_1, pull_dup_152_1, pull_dup_200_1])

create_dupes_graph(0.2, [plum_dup_50_2, plum_dup_100_2, plum_dup_152_2, plum_dup_200_2],
                   [flood_dup_50_2, flood_dup_100_2, flood_dup_152_2, flood_dup_200_2], [pull_dup_50_2, pull_dup_100_2, pull_dup_152_2, pull_dup_200_2])

create_dupes_graph(0.5, [plum_dup_50_5, plum_dup_100_5, plum_dup_152_5, plum_dup_200_5],
                   [flood_dup_50_5, flood_dup_100_5, flood_dup_152_5, flood_dup_200_5], [pull_dup_50_5, pull_dup_100_5, pull_dup_152_5, pull_dup_200_5])
