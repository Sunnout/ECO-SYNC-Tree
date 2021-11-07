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

disk_usage = {}
first_node_dead = 10000000
for proto in protos:
    proto_first_node_dead = float(get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "FIRST_NODE_DEAD"))
    if proto_first_node_dead < first_node_dead:
        first_node_dead = proto_first_node_dead
    usage_list = get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "AVG_DISK_USAGE_PER_SECOND").split(", ")
    disk_usage[proto] = list(map(lambda a: float(a) * 1e-6, usage_list))

plt.rcParams.update({'font.size': 14})
fig = plt.figure()
x = np.arange(len(usage_list))
x = list(map(lambda a: a / 60, x))
plt.xticks(np.arange(min(x), max(x)+1, 2.0))
plt.xlim(right=first_node_dead/60)
plt.xlabel('Time (minutes)')
plt.ylabel('Average Disk Usage (MBytes)')

for proto in protos:
    plt.plot(x[:int(first_node_dead)], disk_usage[proto][:int(first_node_dead)], label=alg_mapper[proto], color=color_mapper[proto])

plt.tight_layout()
plt.legend()
plt.savefig(f'../plots/disk_usage_per_second_{exp_name}_{nodes}_{protos}_{payloads}_{probs}_{runs}.pdf', format='pdf')
plt.close(fig)
