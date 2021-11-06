#!/usr/bin/python3

import sys
from glob import glob
import os
from parseCommonLogs import parse_common_logs

file_name = "../results/{}_{}_{}_{}_{}_{}runs.parsed"
path = "/Volumes/SSD_Pedro/tmp/logs/"
exp_name = sys.argv[1]
base_path = path + exp_name

for node_number_folder in glob(base_path + "/*"):
    node_number = os.path.basename(node_number_folder)
    for proto_folder in glob(node_number_folder + "/*"):
        proto = os.path.basename(proto_folder)
        for payload_folder in glob(proto_folder + "/*"):
            payload = os.path.basename(payload_folder)
            for prob_folder in glob(payload_folder + "/*"):
                prob = os.path.basename(prob_folder)
                runs = glob(prob_folder + "/*")

                print(f"Processing {exp_name}: {proto} with {node_number}, {payload} and {prob} ({len(runs)} runs)")
                res = parse_common_logs(runs)
                with open(file_name.format(exp_name, node_number, proto, payload, prob, len(runs)), 'w') as fp:
                    for key, value in res.items():
                        if type(value) is list:
                            fp.write(key + ":" + str(value)[1:-1] + "\n")
                        else:
                            fp.write(key + ":" + str(value) + "\n")
