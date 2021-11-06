from glob import glob
import os

def parse_logs_plumtree(run_paths):
    for run_path in run_paths:
        node_files = glob(run_path + "/*.log")
        print(f"Found {len(node_files)} node files for {os.path.basename(run_path)}")

        for node_file in node_files:
            file = open(node_file, "r")

            for i in file:
                line = i.split(" ")