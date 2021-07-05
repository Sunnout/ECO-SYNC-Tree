import sys
from parseLogs import parse_logs

# results_file = "{}nodes_{}_{}prob_runs{}.csv"
results_file = "/home/evieira/finalResults/{}nodes_{}_{}prob_runs{}.csv"
file_name = "logs/{}nodes/{}/prob{}/{}runs/node_{}.log"
processes_arg = sys.argv[1]
proto_arg = sys.argv[2]
probs_arg = sys.argv[3]
runs_arg = sys.argv[4]

processes = processes_arg.split(",")
protocols = proto_arg.split(",")
probabilities = probs_arg.split(",")
runs = runs_arg.split(",")

runs_str=''
for run in runs:
    runs_str = runs_str + run

for n_process in processes:
    for proto in protocols:
        for prob in probabilities:
            print(f"Starting to process {proto} with {n_process} nodes and probability {prob} (runs {runs_str})")
            res = parse_logs(file_name, int(n_process), runs, proto, prob)
            res_str = ''
            for item in res:
                res_str = res_str + str(item) + ","
            print(res_str[:-1])
            with open (results_file.format(n_process, proto, prob, runs_str), 'a+') as fp:
              fp.write(res_str[:-1])