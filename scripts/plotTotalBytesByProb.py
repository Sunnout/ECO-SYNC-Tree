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

probs = probs.split(",")
protos = protos.split(",")

bytes = {}
for proto in protos:
    bytes[proto] = []
    for prob in probs:
        bytes[proto].append(
            float(get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, prob, runs), "TOTAL_BYTES")) * 1e-9)

x = np.arange(len(probs))
width = 0.12
plt.rcParams.update({'font.size': 14})
fig = plt.figure()
ax = fig.add_subplot()
ax.set_xticks(x)
ax.set_xticklabels(map(lambda a: "p = " + a, probs))
plt.ylabel('Total Bandwidth Usage (GBytes)')

space = width * len(protos)
idx = 0
for proto in protos:
    ax.bar(x - (space / 2) + idx * width + width / 2, bytes[proto], width, label=alg_mapper[proto],
           color=color_mapper[proto], edgecolor="black")
    idx += 1

plt.tight_layout()
ax.legend()
plt.savefig(f'../plots/bytes/total_gbytes_{exp_name}_{nodes}_{protos}_{payloads}_{probs}_{runs}.pdf', format='pdf')
plt.close(fig)