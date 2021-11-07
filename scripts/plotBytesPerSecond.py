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
for proto in protos:
    first_node_cooldown = float(get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "FIRST_NODE_COOLDOWN"))
    byte_list = get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "TOTAL_BYTES_PER_SECOND").split(", ")
    bytes[proto] = list(map(lambda a: float(a) * 1e-6, byte_list))

plt.rcParams.update({'font.size': 14})
fig = plt.figure()
x = np.arange(len(byte_list))
plt.xlim(right=first_node_cooldown)
plt.xlabel('Time (seconds)')
plt.ylabel('Bandwidth Usage (MBytes)')

for proto in protos:
    plt.plot(x, bytes[proto], label=alg_mapper[proto], color=color_mapper[proto])

plt.tight_layout()
plt.legend()
plt.savefig('../plots/mbytes_per_second.pdf', format='pdf')
plt.close(fig)
