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
    first_node_cooldown = float(get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "FIRST_NODE_COOLDOWN"))
    start_churn = float(get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "START_CHURN"))
    end_churn = float(get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "END_CHURN"))
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
if nodes == "50":
    plt.xlim(right=end_churn/60 + 5, left=start_churn/60 - 1)
elif nodes == "100":
    plt.xlim(right=end_churn/60 + 6, left=start_churn/60 - 1)
else:
    plt.xlim(right=end_churn/60 + 7, left=start_churn/60 - 1)
plt.xlabel('Time (minutes)')
plt.ylabel('Average Synchronization Duration (seconds)')

for proto in protos:
    plt.plot(x[:int(first_node_dead)], syncs[proto][:int(first_node_dead)], label=alg_mapper[proto], color=color_mapper[proto])

plt.tight_layout()
plt.legend()
plt.savefig(f'../plots/sync_time_per_sec/sync_time_per_second_cut_{exp_name}_{nodes}_{protos}_{payloads}_{probs}_{runs}.pdf', format='pdf')
plt.close(fig)
