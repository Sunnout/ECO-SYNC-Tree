import matplotlib.pyplot as plt
import numpy as np
import seaborn as sb
import math
sb.set()
sb.set_style("darkgrid", {"axes.facecolor": ".9"})

class GraphBuilder:

    FONT = {'fontname':'Arial'}
    COLOURS = ['navajowhite','orange','lightgreen','green']

    RELIABILITY_FOLDER = 'reliabilities/'
    LATENCY_FOLDER = 'latencies/'

    EAGER_CYCLON_TITLE = 'Eager Push with Cyclon'
    EAGER_HYPAR_TITLE = 'Eager Push with HyParView'
    PLUM_CYCLON_TITLE = 'Plumtree with Cyclon'
    PLUM_HYPAR_TITLE = 'Plumtree with HyParView'

    S_MSG_B_TIME_TITLE = 'Small Messages with big broadcast interval'
    B_MSG_B_TIME_TITLE = 'Big Messages with big broadcast interval'
    S_MSG_S_TIME_TITLE = 'Small Messages with small broadcast interval'
    B_MSG_S_TIME_TITLE = 'Big Messages with small broadcast interval'

    EAGER_CYCLON_FILE = 'eager_cyclon.pdf'
    EAGER_HYPAR_FILE = 'eager_hypar.pdf'
    PLUM_CYCLON_FILE = 'plum_cyclon.pdf'
    PLUM_HYPAR_FILE = 'plum_hypar.pdf'

    S_MSG_B_TIME_FILE = 'small_mess_big_time.pdf'
    B_MSG_B_TIME_FILE = 'big_mess_big_time.pdf'
    S_MSG_S_TIME_FILE = 'small_mess_small_time.pdf'
    B_MSG_S_TIME_FILE = 'big_mess_small_time.pdf'

    def __init__(self, results, project_path):
        self.eager_cyclon_results = [results[0], results[1], results[2], results[3]]
        self.eager_hypar_results = [results[4], results[5], results[6], results[7]]
        self.plumtree_cyclon_results = [results[8], results[9], results[10], results[11]]
        self.plumtree_hypar_results = [results[12], results[13], results[14], results[15]]

        self.n_combs_protocols = 4
        self.n_combs_parameters = 4

        self.latency_index = 0
        self.reliability_index = 1
        self.transmitted_index = 2
        self.plumTree_index = 3

        self.m_T_index = 0
        self.M_T_index = 1
        self.m_t_index = 2
        self.M_t_index = 3

        self.graph_path = project_path + '/graphs/'

    def graphs_same_protocol(self, protocol_comb):
        data = []
        x = ["Small Messages\nBig Time", "Big Messages\nBig Time", "Small Messages\nSmall Time", "Big Messages\nSmall Time"]
        latencies = []
        reliabilities = []

        if(protocol_comb == 0):
            data = self.eager_cyclon_results
            title = self.EAGER_CYCLON_TITLE
            file_name = self.EAGER_CYCLON_FILE

        elif(protocol_comb == 1):
            data = self.eager_hypar_results
            title = self.EAGER_HYPAR_TITLE
            file_name = self.EAGER_HYPAR_FILE

        elif(protocol_comb == 2):
            data = self.plumtree_cyclon_results
            title = self.PLUM_CYCLON_TITLE
            file_name = self.PLUM_CYCLON_FILE

        else:
            data = self.plumtree_hypar_results
            title = self.PLUM_HYPAR_TITLE
            file_name = self.PLUM_HYPAR_FILE

        for results in data:
            latencies.append(results[self.latency_index])
            reliabilities.append(results[self.reliability_index])

        self.__create_graph(x, latencies, 'Latency: ' + title, 'ms', self.LATENCY_FOLDER, file_name)
        self.__create_graph(x, reliabilities, 'Reliability: ' + title, '%', self.RELIABILITY_FOLDER, file_name)

    def graphs_between_protocols(self, parameters_comb):
        x = ["Eager Push\nwith Cyclon", "Eager Push\nwith HyParView", "Plumtree\nwith Cyclon", "Plumtree\nwith HyParView"]
        latencies = []
        reliabilities = []

        if(parameters_comb == 0):
            title = self.S_MSG_B_TIME_TITLE
            file_name = self.S_MSG_B_TIME_FILE

        elif(parameters_comb == 1):
            title = self.B_MSG_B_TIME_TITLE
            file_name = self.B_MSG_B_TIME_FILE

        elif(parameters_comb == 2):
            title = self.S_MSG_S_TIME_TITLE
            file_name = self.S_MSG_S_TIME_FILE

        elif(parameters_comb == 3):
            title = self.B_MSG_S_TIME_TITLE
            file_name = self.B_MSG_S_TIME_FILE

        latencies.append(self.eager_cyclon_results[parameters_comb][self.latency_index])
        latencies.append(self.eager_hypar_results[parameters_comb][self.latency_index])
        latencies.append(self.plumtree_cyclon_results[parameters_comb][self.latency_index])
        latencies.append(self.plumtree_hypar_results[parameters_comb][self.latency_index])

        reliabilities.append(self.eager_cyclon_results[parameters_comb][self.reliability_index])
        reliabilities.append(self.eager_hypar_results[parameters_comb][self.reliability_index])
        reliabilities.append(self.plumtree_cyclon_results[parameters_comb][self.reliability_index])
        reliabilities.append(self.plumtree_hypar_results[parameters_comb][self.reliability_index])

        self.__create_graph(x, latencies, 'Latency: ' + title, 'ms', self.LATENCY_FOLDER, file_name)
        self.__create_graph(x, reliabilities, 'Reliability: ' + title, '%', self.RELIABILITY_FOLDER, file_name)

    def reliability_all(self):
        n = 4
        ind = np.arange(n)
        width = 0.15

        eager_cyclon = []
        eager_hypar = []
        plum_cyclon = []
        plum_hypar = []

        for parameter_comb in range(self.n_combs_parameters):
            if(parameter_comb == 1):
                comb = 2

            elif(parameter_comb == 2):
                comb = 1

            else:
                comb = parameter_comb

            eager_cyclon.append(self.eager_cyclon_results[comb][self.reliability_index])
            eager_hypar.append(self.eager_hypar_results[comb][self.reliability_index])
            plum_cyclon.append(self.plumtree_cyclon_results[comb][self.reliability_index])
            plum_hypar.append(self.plumtree_hypar_results[comb][self.reliability_index])

        fig = plt.figure()
        ax = fig.add_subplot(111)
        rects1 = ax.bar(ind - width, eager_cyclon, width, color='lightblue', edgecolor='black')
        rects2 = ax.bar(ind, eager_hypar, width, color='steelblue', edgecolor='black')
        rects3 = ax.bar(ind + width, plum_cyclon, width, color='lightgray', edgecolor='black')
        rects4 = ax.bar(ind + 2 * width, plum_hypar, width, color='dimgray', edgecolor='black')
        ax.set_title('Average Broadcast Reliability (%)')
        ax.set_xticks(ind + width / 2)
        ax.set_xticklabels(('Small Payload\n&\nBig Interval', 'Small Payload\n&\nSmall Interval', 'Big Payload\n&\nBig Interval', 'Big Payload\n&\nSmall Interval'), fontsize='x-small')
        ax.legend((rects1[0], rects2[0], rects3[0], rects4[0]), ("Eager Push\nwith Cyclon", "Eager Push\nwith HyParView", "Plumtree\nwith Cyclon", "Plumtree\nwith HyParView"), fontsize='x-small')
        plt.savefig(self.graph_path + '/reliabilities/all.pdf', format='pdf')

    def latency_all(self):
        n = 4
        ind = np.arange(n)
        width = 0.15

        eager_cyclon = []
        eager_hypar = []
        plum_cyclon = []
        plum_hypar = []

        for parameter_comb in range(self.n_combs_parameters):
            if(parameter_comb == 1):
                comb = 2

            elif(parameter_comb == 2):
                comb = 1

            else:
                comb = parameter_comb

            eager_cyclon.append(self.eager_cyclon_results[comb][self.latency_index])
            eager_hypar.append(self.eager_hypar_results[comb][self.latency_index])
            plum_cyclon.append(self.plumtree_cyclon_results[comb][self.latency_index])
            plum_hypar.append(self.plumtree_hypar_results[comb][self.latency_index])

        fig = plt.figure()
        ax = fig.add_subplot(111)
        rects1 = ax.bar(ind - width, eager_cyclon, width, color='lightblue', edgecolor='black')
        rects2 = ax.bar(ind, eager_hypar, width, color='steelblue', edgecolor='black')
        rects3 = ax.bar(ind + width, plum_cyclon, width, color='lightgray', edgecolor='black')
        rects4 = ax.bar(ind + 2 * width, plum_hypar, width, color='dimgray', edgecolor='black')
        ax.set_title('Average Broadcast Latency (ms)')
        ax.set_xticks(ind + width / 2)
        ax.set_xticklabels(('Small Payload\n&\nBig Interval', 'Small Payload\n&\nSmall Interval', 'Big Payload\n&\nBig Interval', 'Big Payload\n&\nSmall Interval'), fontsize='x-small')
        ax.legend((rects1[0], rects2[0], rects3[0], rects4[0]), ("Eager Push\nwith Cyclon", "Eager Push\nwith HyParView", "Plumtree\nwith Cyclon", "Plumtree\nwith HyParView"), fontsize='x-small')
        plt.savefig(self.graph_path + '/latencies/all.pdf', format='pdf')


    def __create_graph(self, x, data, title, units, folder, file_name):
        _ , ax = plt.subplots()
        ax = plt.bar(width=0.7, x=x, height=data, color=self.COLOURS)

        if(units != ''):
            title += " ({})".format(units)

        plt.title(title, **self.FONT)
        maxVal = max(data)
        plt.axis([None, None, None, maxVal + 0.05 * maxVal])
        plt.yticks([])
        xlocs, _ = plt.xticks()
        xlocs=[0,1,2,3]
        plt.xticks(xlocs, **self.FONT)

        for i, v in enumerate(data):
            if(data[i] >= 100):
                plt.text(xlocs[i] - 0.22, v + 1, str("{:.2f}".format(v)))
            else:
                plt.text(xlocs[i] - 0.17, v + 1, str("{:.2f}".format(v)))

        plt.savefig(self.graph_path + folder + file_name, format='pdf')

    def create_messages_bytes_graphs(self):
        eager_cyclon = []
        eager_hypar = []
        plum_cyclon = []
        plum_hypar = []

        for i in range(2):
            eager_cyclon.append([])
            eager_hypar.append([])
            plum_cyclon.append([])
            plum_hypar.append([])

        for parameter_comb in range(self.n_combs_parameters):
            if(parameter_comb == 1):
                comb = 2

            elif(parameter_comb == 2):
                comb = 1

            else:
                comb = parameter_comb

            mt1, _, bt1, _ = self.eager_cyclon_results[comb][self.transmitted_index]
            eager_cyclon[0].append(mt1)
            eager_cyclon[1].append(bt1)

            mt2, _, bt2, _ = self.eager_hypar_results[comb][self.transmitted_index]
            eager_hypar[0].append(mt2)
            eager_hypar[1].append(bt2)

            mt3, _, bt3, _ =  self.plumtree_cyclon_results[comb][self.transmitted_index]
            plum_cyclon[0].append(mt3)
            plum_cyclon[1].append(bt3)

            mt4, _, bt4, _ =  self.plumtree_hypar_results[comb][self.transmitted_index]
            plum_hypar[0].append(mt4)
            plum_hypar[1].append(bt4)

        self.__create_messages_graph(eager_cyclon[0], eager_hypar[0], plum_cyclon[0], plum_hypar[0])
        self.__create_bytes_graph(eager_cyclon[1], eager_hypar[1], plum_cyclon[1], plum_hypar[1])

    def __create_messages_graph(self, eager_cyclon, eager_hypar, plum_cyclon, plum_hypar):
        n = 4
        ind = np.arange(n)
        width = 0.15

        print(eager_cyclon)
        print(eager_hypar)
        print(plum_cyclon)
        print(plum_hypar)

        fig = plt.figure()
        ax = fig.add_subplot(111)
        rects1 = ax.bar(ind - width, eager_cyclon, width, color='lightblue', edgecolor='black')
        rects2 = ax.bar(ind, eager_hypar, width, color='steelblue', edgecolor='black')
        rects3 = ax.bar(ind + width, plum_cyclon, width, color='lightgray', edgecolor='black')
        rects4 = ax.bar(ind + 2 * width, plum_hypar, width, color='dimgray', edgecolor='black')
        ax.set_title('Number of Messages Transmitted')
        ax.set_xticks(ind + width / 2)
        ax.set_xticklabels(('Small Payload\n&\nBig Interval', 'Small Payload\n&\nSmall Interval', 'Big Payload\n&\nBig Interval', 'Big Payload\n&\nSmall Interval'), fontsize='x-small')
        ax.legend((rects1[0], rects2[0], rects3[0], rects4[0]), ("Eager Push\nwith Cyclon", "Eager Push\nwith HyParView", "Plumtree\nwith Cyclon", "Plumtree\nwith HyParView"), fontsize='x-small')
        plt.yscale("log")
        plt.savefig(self.graph_path + '/messages_bytes/messages.pdf', format='pdf')

    def __create_bytes_graph(self, eager_cyclon, eager_hypar, plum_cyclon, plum_hypar):
        n = 4
        ind = np.arange(n)
        width = 0.15

        fig = plt.figure()
        ax = fig.add_subplot(111)
        rects1 = ax.bar(ind - width, eager_cyclon, width, color='lightblue', edgecolor='black')
        rects2 = ax.bar(ind, eager_hypar, width, color='steelblue', edgecolor='black')
        rects3 = ax.bar(ind + width, plum_cyclon, width, color='lightgray', edgecolor='black')
        rects4 = ax.bar(ind + 2 * width, plum_hypar, width, color='dimgray', edgecolor='black')
        ax.set_title('Number of Bytes Transmitted')
        ax.set_xticks(ind + width / 2)
        ax.set_xticklabels(('Small Payload\n&\nBig Interval', 'Small Payload\n&\nSmall Interval', 'Big Payload\n&\nBig Interval', 'Big Payload\n&\nSmall Interval'), fontsize='x-small')
        ax.legend((rects1[0], rects2[0], rects3[0], rects4[0]), ("Eager Push\nwith Cyclon", "Eager Push\nwith HyParView", "Plumtree\nwith Cyclon", "Plumtree\nwith HyParView"), fontsize='x-small')
        plt.yscale("log")
        plt.savefig(self.graph_path + '/messages_bytes/bytes.pdf', format='pdf')