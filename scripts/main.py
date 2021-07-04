import os
import sys
from parseLogs import parse_logs

results_file = "{}nodes_{}_{}prob.csv"
file_name = "/logs/{}nodes/{}⁄prob{}/{}runs⁄node_{}.log"
processes_arg = sys.argv[1]
proto_arg = sys.argv[2]
probs_arg = sys.argv[3]
n_runs = int(sys.argv[4])

processes = processes_arg.split(",")
protocols = proto_arg.split(",")
probabilities = probs_arg.split(",")

for n_process in processes:
    for proto in protocols:
        for prob in probabilities:
            res = parse_logs(file_name, int(n_process), n_runs, proto, float(prob))
            res_str = ''
            for item in res:
                res_str = res_str + str(item) + ","
            with open (results_file.format(n_process, proto, prob), 'w') as fp:
              fp.write(res_str[:-1])