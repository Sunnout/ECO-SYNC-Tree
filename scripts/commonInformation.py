

def get_value_by_key(file_name, key):
    file = open(file_name, "r")
    for i in file:
        line = i.split(":")

        if line[0] == key:
            return line[1].strip()
    print(f"Key {key} not found in file {file_name}")
    exit()


file_name = "../results/{}_{}nodes_{}_payload{}_prob{}_{}runs.parsed"
alg_mapper = {"plumtree": "SYNC Tree",
              "flood": "Causal Flood",
              "periodicpull": "Periodic Sync (1000 ms)",
              "periodicpullsmallertimer": "Periodic Sync (200 ms)",
              "plumtreegc": "ECO SYNC Tree"}

color_mapper = {"plumtree": 'dodgerblue',
                "flood": 'darkgreen',
                "periodicpull": 'crimson',
                "periodicpullsmallertimer": 'orange',
                "plumtreegc": "deeppink"}