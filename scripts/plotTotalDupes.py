import sys
import numpy as np
import matplotlib.pyplot as plt
from commonInformation import *


exp_name = sys.argv[1]
nodes = sys.argv[2]
protos = sys.argv[3]
payloads = sys.argv[4]
probs = sys.argv[5]
runs = sys.argv[6]

nodes = nodes.split(",")
protos = protos.split(",")

dupes = {}
for proto in protos:
    dupes[proto] = []
    for node in nodes:
        dupes[proto].append(
            float(get_value_by_key(file_name.format(exp_name, node, proto, payloads, probs, runs), "TOTAL_DUPES")))

x = np.arange(len(nodes))
width = 0.12
plt.rcParams.update({'font.size': 14})
fig = plt.figure()
ax = fig.add_subplot()
ax.set_xticks(x)
ax.set_xticklabels(map(lambda a: a + " nodes", nodes))
plt.ylabel('Total Duplicate Messages Received')
plt.yscale("log")

space = width * len(protos)
idx = 0
for proto in protos:
    ax.bar(x - (space / 2) + idx * width + width / 2, dupes[proto], width, label=alg_mapper[proto],
           color=color_mapper[proto], edgecolor="black")
    idx += 1

plt.tight_layout()
ax.legend()
plt.savefig(f'../plots/dupes_{exp_name}_{nodes}_{protos}_{payloads}_{probs}_{runs}.pdf', format='pdf')
plt.close(fig)
