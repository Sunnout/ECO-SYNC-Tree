import os

import sys
import glob

# results_file = "{}nodes_{}_{}prob_runs{}.csv"
logs_folder_template = "/tmp/logs/{}nodes/{}/prob{}/{}runs"
# file_name = "logs/{}nodes/{}/prob{}/{}runs/node_{}.log"
processes_arg = sys.argv[1]
proto_arg = sys.argv[2]
probs_arg = sys.argv[3]
runs_arg = sys.argv[4]

processes = processes_arg.split(",")
protocols = proto_arg.split(",")
probabilities = probs_arg.split(",")
runs = runs_arg.split(",")


def progressBar(current, total, barLength=20):
    percent = float(current) * 100 / total
    arrow = '-' * int(percent / 100 * barLength - 1) + '>'
    spaces = ' ' * (barLength - len(arrow))
    print('Progress: [%s%s] %d %%' % (arrow, spaces, percent), end='\r')


def process_file(file_name, file_path, results):
    with open(file_path, "r") as a_file:
        for line in a_file:
            split = line.strip().split(None, 5)
            if len(split) >= 6 and split[3] == "[CRDT-VAL]":
                crdt_name = split[4]
                crdt_val = split[5]
                if not crdt_name in results:
                    results[crdt_name] = {}
                if crdt_val not in results[crdt_name]:
                    results[crdt_name][crdt_val] = []
                results[crdt_name][crdt_val].append(file_name)
                # print(split[4])


for n_process in processes:
    for proto in protocols:
        for prob in probabilities:
            for run in runs:
                wrong = []
                results = {}
                print(f"Starting to process {proto} with {n_process} nodes and probability {prob} (run {run})")
                logs_folder = logs_folder_template.format(n_process, proto, prob, run)
                all_files = glob.glob(f"{logs_folder}/node_*.log")
                if len(all_files) == 0:
                    print("No files found")
                    exit(1)
                counter = 0
                for file_path in all_files:
                    progressBar(counter, len(all_files))
                    counter += 1
                    file_name = os.path.basename(file_path)
                    if os.path.isfile(file_path):
                        process_file(file_name, file_path, results)
                print(" ")
                for c, r in results.items():
                    print(str(c))
                    if len(r) > 1:
                        wrong.append(c)
                    for state, nodes in r.items():
                        if len(r) > 1:
                            print(f"\t{len(nodes)}x -> {state} ({nodes})")
                        else:
                            print(f"\t{len(nodes)}x -> {state}")
                    # print(" ")
                if len(wrong) > 0:
                    print(f"!!!!!!!!!!!!!!!!!!! Not converged, wrong protocols.replication.crdts {wrong}")
                else:
                    print("All ok!")
