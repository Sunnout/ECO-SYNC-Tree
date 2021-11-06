import sys
import numpy as np
import matplotlib.pyplot as plt


def getValueByKey(file_name, key):
    file = open(file_name, "r")
    for i in file:
        line = i.split(":")

        if line[0] == key:
            return line[1].strip()
    print(f"Key {key} not found in file {file_name}")
    exit()


file_name = "../results/{}_{}nodes_{}_payload{}_prob{}_{}runs.parsed"
alg_mapper = {"plumtree": "Causal Plumtree",
              "flood": "Causal Flood",
              "periodicpull": "Periodic Pull (1000 ms)",
              "periodicpullsmallertimer": "Periodic Pull (200 ms)",
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
            float(getValueByKey(file_name.format(exp_name, node, proto, payloads, probs, runs), "AVG_BCAST_LATENCY")))

x = np.arange(len(nodes))
width = 0.1
plt.rcParams.update({'font.size': 14})
fig = plt.figure()
ax = fig.add_subplot()
ax.set_xticks(x)
ax.set_xticklabels(map(lambda a: a + " nodes", nodes))
plt.ylabel('Average Broadcast Latency (seconds)')

space = width * len(protos)
idx = 0
for proto in protos:
    ax.bar(x - (space / 2) + idx * width + width / 2, latencies[proto], width, label=alg_mapper[proto],
           color=color_mapper[proto], edgecolor="black")
    idx += 1

plt.tight_layout()
ax.legend()
plt.savefig('../plots/latency.pdf', format='pdf')
plt.close(fig)
