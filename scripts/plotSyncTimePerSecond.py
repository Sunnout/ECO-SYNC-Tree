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
for proto in protos:
    catastrophe_start = float(get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "START_CATASTROPHE"))
    stabilization_time = float(get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "TREE_STABILIZATION_TIME"))
    sync_list = get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "AVG_SYNC_TIME_PER_SECOND").split(", ")
    syncs[proto] = list(map(float, sync_list))

plt.rcParams.update({'font.size': 14})
fig = plt.figure()
x = np.arange(len(sync_list))
plt.xlim(right=catastrophe_start + stabilization_time + 20)
plt.xlabel('Time (seconds)')
plt.ylabel('Average Synchronization Duration (seconds)')

for proto in protos:
    plt.plot(x, syncs[proto], label=alg_mapper[proto], color=color_mapper[proto])

plt.tight_layout()
plt.legend()
plt.savefig('../plots/sync_time_per_second.pdf', format='pdf')
plt.close(fig)
