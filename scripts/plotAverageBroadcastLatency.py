#!/usr/bin/python3

import sys
import numpy as np


def getValueByKey(file_name, key):
    file = open(file_name, "r")
    for i in file:
        line = i.split(":")

        if line[0] == key:
            return line[1]
    print(f"Key {key} not found in file {file_name}")
    exit()


file_name = "../results/{}_{}nodes_{}_payload{}_prob{}_{}runs.parsed"
alg_mapper = {"plumtree": "Causal Plumtree",
              "flood": "Causal Flood",
              "periodicpull": "Periodic Pull (1000ms)",
              "periodicpullsmallertimer": "Periodic Pull (200ms)",
              "plumtreegc": "Causal PlumtreeGC"}
color_mapper = {"plumtree": '#009E73',
                "flood": '#E69F00',
                "periodicpull": '#9400D3',
                "periodicpullsmallertimer": '#56B4E9'}


exp_name = sys.argv[1]
nodes = sys.argv[2]
protos = sys.argv[3]
payloads = sys.argv[4]
probs = sys.argv[5]
runs = sys.argv[6]

nodes = nodes.split(",")
protos = protos.split(",")

latencies = {}
for proto in protos:
    latencies[proto] = []
    for node in nodes:
        latencies[proto].append(
            getValueByKey(file_name.format(exp_name, node, proto, payloads, probs, runs), "AVG_BCAST_LATENCY"))

x = np.arange(len(nodes))
width = 0.2
fig, ax = plt.subplots()
ax.tick_params(axis='both', which='major', labelsize='large')
ax.tick_params(axis='both', which='minor', labelsize='large')
ax.set_xticks(x)
ax.set_xticklabels(nodes, fontsize='x-large')
plt.ylabel('Average Broadcast Latency (seconds)', fontsize='xx-large')

space = width * len(protos)
idx = 0
for proto in protos:
    ax.bar(x - (space / 2) + idx * width, latencies[proto], width, label=alg_mapper[proto], fontsize='x-large',
           framealpha=0.5)
    idx += 1

plt.savefig('../plots/latency_prob{}.pdf'.format(probability), format='pdf')
plt.close(fig)
