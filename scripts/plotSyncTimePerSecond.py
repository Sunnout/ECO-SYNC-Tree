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

syncs = {}
first_node_dead = 10000000
for proto in protos:
    proto_first_node_dead = float(get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "FIRST_NODE_DEAD"))
    if proto_first_node_dead < first_node_dead:
        first_node_dead = proto_first_node_dead
    sync_list = get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "AVG_SYNC_TIME_PER_SECOND").split(", ")
    syncs[proto] = list(map(float, sync_list))

plt.rcParams.update({'font.size': 14})
fig = plt.figure(figsize=(10,5))
x = np.arange(len(sync_list))
x = list(map(lambda a: a / 60, x))
plt.xticks(np.arange(min(x), max(x)+1, 1.0))
plt.xlim(right=first_node_dead/60)
plt.xlabel('Time (minutes)')
plt.ylabel('Average Synchronisation Duration (seconds)')

for proto in protos:
    plt.plot(x[:int(first_node_dead)], syncs[proto][:int(first_node_dead)], label=alg_mapper[proto], color=color_mapper[proto])

plt.tight_layout()
plt.legend()
plt.savefig(f'../plots/sync_time_per_sec/sync_time_per_second_{exp_name}_{nodes}_{protos}_{payloads}_{probs}_{runs}.pdf', format='pdf')
plt.close(fig)
