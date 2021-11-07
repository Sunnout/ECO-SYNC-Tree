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

latencies = {}

first_node_dead = 10000000
for proto in protos:
    proto_first_node_dead = float(get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "FIRST_NODE_DEAD"))
    if proto_first_node_dead < first_node_dead:
        first_node_dead = proto_first_node_dead
    lat_list = get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "AVG_LATENCIES_PER_SECOND").split(", ")
    latencies[proto] = list(map(float, lat_list))

plt.rcParams.update({'font.size': 14})
fig = plt.figure(figsize=(10,5))
x = np.arange(len(lat_list))
x = list(map(lambda a: a / 60, x))
plt.xticks(np.arange(min(x), max(x)+1, 1.0))
plt.xlim(right=first_node_dead/60)
plt.xlabel('Time (minutes)')
plt.ylabel('Average Broadcast Latency (seconds)')

for proto in protos:
    plt.plot(x[:int(first_node_dead)], latencies[proto][:int(first_node_dead)], label=alg_mapper[proto], color=color_mapper[proto])

plt.tight_layout()
plt.legend()
plt.savefig(f'../plots/latency_per_second_{exp_name}_{nodes}_{protos}_{payloads}_{probs}_{runs}.pdf', format='pdf')
plt.close(fig)
