import os
from plumtreeMessagesScript import plumtree_messages
from totalTransmittedReceivedScript import messages_bytes
from averageBroadcastLatency import avg_latency
from graphFunctions import GraphBuilder

file_name = "node{}-t{}-{}.log"

results = {0: [525.5888888888888, 98.68696296296298, (4065639.3333333335, 4064628.6666666665, 4301966000.0, 4300892888.0)], 
        1: [514.3004999074245, 96.1465680202375, (3783352.6666666665, 3472192.6666666665, 376758594698.0, 345856125969.3333)], 
        2: [535.607862533483, 98.69679375544273, (13856320.666666666, 13844615.0, 14709495314.666666, 14697079971.666666)], 
        3: [286.4351091516239, 34.80087693883797, (3165216.6666666665, 2372670.6666666665, 315470407324.6667, 236428206034.0)], 
        4: [633.7674444444445, 95.8941111111111, (1814682.6666666667, 1814398.3333333333, 1886754573.3333333, 1886472275.0)], 
        5: [631.2339913336542, 98.26153248404499, (1909221.6666666667, 1908635.3333333333, 186644712858.33334, 186588422288.0)], 
        6: [660.2600859990445, 96.36187698150803, (6023259.666666667, 6021070.666666667, 6359760761.666667, 6357461544.666667)], 
        7: [321.74288285340316, 41.2212647969205, (2030812.0, 1841052.0, 200247764329.33334, 181330639971.33334)], 
        8: [451.1537407407407, 99.1574814814815, (5152973.666666667, 5151865.0, 5405889583.0, 5404703291.0)], 
        9: [475.59480134572254, 84.58659032424663, (3538320.0, 3072071.3333333335, 342403042022.6667, 296580710054.6667)], 
        10: [494.6987438185642, 93.91975294411104, (60065937.333333336, 56126425.666666664, 57623291458.333336, 53511476765.666664)], 
        11: [251.59077077691856, 25.831720743708388, (2510755.3333333335, 1748207.6666666667, 238615516249.33334, 163921266571.0)], 
        12: [449.2213539737797, 83.08457773169748, (2408524.6666666665, 2408100.6666666665, 1735043736.6666667, 1734697490.6666667)], 
        13: [458.66849355651016, 86.25480980548168, (2545645.6666666665, 2545283.6666666665, 165207462398.33334, 165166174861.0)], 
        14: [466.2687799295579, 88.39489098562916, (8427868.666666666, 8426449.666666666, 6208544460.666667, 6207299622.333333)],
        15: [390.43127612490775, 56.05924051446864, (3180556.5, 3161240.5, 207685375294.5, 206224452747.0)]}

n_processes = 100
n_combinations = 16
n_runs = 2
n_protocol_combs = 4
n_parameter_combs = 4

"""

logs_folder = os.getcwd() + "/final_logs/"
file_path = logs_folder + file_name

for comb in [16]:
    
    print('Combination: {}\n'.format(comb))
    results[comb-1] = []
    
    print('Analyzing reliability...')
    rel = reliability(file_path, n_processes, n_runs, comb, True)
    
    print('Analyzing transmission content...')
    transm = messages_bytes(file_path, n_processes, n_runs, comb, True)
    
    print('Analyzing latency...')
    lat = avg_latency(file_path, n_processes, n_runs, comb, True)
    
    results[comb-1].append(lat)
    results[comb-1].append(rel)
    results[comb-1].append(transm)

    #if(comb > 8):
    #    print('Analyzing Plumtree...')
    #    plum = plumtree_messages(file_path, n_processes, n_runs, comb, True)
    #    results[comb-1].append(plum)

print(results)

graphBuilder = GraphBuilder(results, os.getcwd())

for comb in range(n_parameter_combs):
    graphBuilder.graphs_between_protocols(comb)

for comb in range(n_protocol_combs):
    graphBuilder.graphs_same_protocol(comb)


graphBuilder.create_messages_bytes_graphs()
graphBuilder.reliability_all()
graphBuilder.latency_all()
"""

for comb in range(n_combinations):

    _, _, bt, br = results[comb][2]
    perc_bytes_lost = ((bt-br) / bt) * 100
    print('t{}: {:.2f}'.format(comb+1, perc_bytes_lost))