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

protos = protos.split(",")

latencies = {}
for proto in protos:
    lat_list = getValueByKey(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "AVG_LATENCIES_PER_SECOND").split(", ")
    latencies[proto] = list(map(float, lat_list))

plt.rcParams.update({'font.size': 14})
fig = plt.figure()
x = np.arange(len(lat_list))
plt.xlabel('Time (seconds)')
plt.ylabel('Average Broadcast Latency (seconds)')

for proto in protos:
    plt.plot(x, latencies[proto], label=alg_mapper[proto], color=color_mapper[proto])

plt.tight_layout()
plt.legend()
plt.savefig('../plots/latency_per_second.pdf', format='pdf')
plt.close(fig)
