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
for proto in protos:
    lat_list = get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "AVG_LATENCIES_PER_SECOND").split(", ")
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
