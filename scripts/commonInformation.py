

def get_value_by_key(file_name, key):
    file = open(file_name, "r")
    for i in file:
        line = i.split(":")

        if line[0] == key:
            return line[1].strip()
    print(f"Key {key} not found in file {file_name}")
    exit()


file_name = "../results/{}_{}nodes_{}_payload{}_prob{}_{}runs.parsed"
alg_mapper = {"plumtree": "Causal Plumtree",
              "flood": "Causal Flood",
              "periodicpull": "Periodic Pull (1000 ms)",
              "periodicpullsmallertimer": "Periodic Pull (200 ms)",
              "plumtreegc": "Causal PlumtreeGC"}
color_mapper = {"plumtree": '#009E73',
                "flood": '#E69F00',
                "periodicpull": '#9400D3',
                "periodicpullsmallertimer": '#56B4E9',
                "plumtreegc": "black"}