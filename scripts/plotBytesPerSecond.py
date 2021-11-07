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

protos = protos.split(",")

bytes = {}
first_node_dead = 10000000
for proto in protos:
    proto_first_node_dead = float(get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "FIRST_NODE_DEAD"))
    if proto_first_node_dead < first_node_dead:
        first_node_dead = proto_first_node_dead
    byte_list = get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "TOTAL_BYTES_PER_SECOND").split(", ")
    bytes[proto] = list(map(lambda a: float(a) * 1e-6, byte_list))

plt.rcParams.update({'font.size': 14})
fig = plt.figure(figsize=(10,5))
x = np.arange(len(byte_list))
x = list(map(lambda a: a / 60, x))
plt.xticks(np.arange(min(x), max(x)+1, 1.0))
plt.xlim(right=first_node_dead/60)
plt.xlabel('Time (minutes)')
plt.ylabel('Bandwidth Usage (MBytes)')

for proto in protos:
    plt.plot(x[:int(first_node_dead)], bytes[proto][:int(first_node_dead)], label=alg_mapper[proto], color=color_mapper[proto])

#plt.vlines(400, 0, plt.gca().get_ylim()[1], linestyles="dashdot", color="grey")
#plt.text(405, -2, "oi", verticalalignment='center', fontsize=12)
plt.tight_layout()
plt.legend()
plt.savefig(f'../plots/mbytes_per_second_{exp_name}_{nodes}_{protos}_{payloads}_{probs}_{runs}.pdf', format='pdf')
plt.close(fig)
