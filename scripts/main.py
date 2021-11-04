import sys
from logParserPlumtree import parse_logs_plumtree
from logParserFlood import parse_logs_flood
from logParserPeriodicPull import parse_logs_periodicpull

exp_name = sys.argv[1]
nodes_arg = sys.argv[2]
proto_arg = sys.argv[3]
payloads_arg = sys.argv[4]
probs_arg = sys.argv[5]
runs_arg = sys.argv[6]

exp_name = exp_name.split(",")
nodes = processes_arg.split(",")
protocols = proto_arg.split(",")
payloads = payloads_arg.split(",")
probabilities = probs_arg.split(",")
runs = runs_arg.split(",")

runs_str = ''
for run in runs:
    runs_str = runs_str + run

for node_number in nodes:
    for proto in protocols:
        for payload in payloads:
            for prob in probabilities:
                print(f"Processing {exp_name}: {proto} with {node_number} nodes, payload {payload} and probability {prob} (runs {runs_str})")

                if proto == "plumtree":
                    parse_logs_plumtree(int(node_number), runs, payload, prob)
                elif proto == "flood":
                    parse_logs_flood(int(node_number), runs, payload, prob)
                elif proto == "periodicpull":
                    parse_logs_periodic_pull("", int(node_number), runs, payload, prob)
                elif proto == "periodicpullsmallertimer":
                    parse_logs_periodic_pull("smallertimer", int(node_number), runs, payload, prob)