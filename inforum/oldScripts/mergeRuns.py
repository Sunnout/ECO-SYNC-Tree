import numpy as np
import pandas as pd
import sys


processes_arg = sys.argv[1]
proto_arg = sys.argv[2]
probs_arg = sys.argv[3]

f1 = np.genfromtxt("../results/{}nodes_{}_{}prob_runs1.csv".format(processes_arg, proto_arg, probs_arg), delimiter=',')
f2 = np.genfromtxt("../results/{}nodes_{}_{}prob_runs2.csv".format(processes_arg, proto_arg, probs_arg), delimiter=',')
f3 = np.genfromtxt("../results/{}nodes_{}_{}prob_runs3.csv".format(processes_arg, proto_arg, probs_arg), delimiter=',')


f1N = np.zeros(len(f1))
f2N = np.zeros(len(f2))
f3N = np.zeros(len(f3))

for i in range(len(f1)):
    f1N[i] = float(f1[i])
    f2N[i] = float(f2[i])
    f3N[i] = float(f3[i])

tmp = np.add(f1N, f2N)
tmp2 = np.add(tmp, f3N)
f = np.divide(tmp2, 3)

str = ','.join(['%.5f' % num for num in f])

print(str)

with open("../results/{}nodes_{}_{}prob_runs123.csv".format(processes_arg, proto_arg, probs_arg), 'a+') as fp:
    fp.write(str)


