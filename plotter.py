#!/usr/bin/env python2
import sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from csv import DictReader
from copy import copy
from collections import defaultdict

#regular
LINE_STYLE = ['b:+', 'g-', 'r-s', 'c--', 'k-.', 'r--', 'g-x']
#increasingk
#LINE_STYLE = ['b--s', 'b-s', 'g--x', 'g-x', 'r--', 'r-', 'k-.']
#reqnum
#LINE_STYLE = ['b-.', 'r--', 'g-', 'c:+', 'k:']

def main():
    for file in sys.argv[1:]:
        f = open(file, 'r')
        d = DictReader(f)
        data = defaultdict(list)
        for row in d:
            for key, value in row.iteritems():
                data[key].append(float(value))

        cols = copy(d.fieldnames)
        cols.remove("sys_util")

        # Sort data by sys_util
        tmp = zip(data['sys_util'], range(0,len(data['sys_util'])))
        tmp = sorted(tmp)
        data_sorted = defaultdict(list)
        for row in d.fieldnames:
            for i in tmp:
                data_sorted[row].append(data[row][i[1]])



        plt.figure(figsize=(8,4))
        for style, col in enumerate(cols):
            plt.plot(data_sorted["sys_util"], data_sorted[col], LINE_STYLE[style], label=col, linewidth=2.0)

        plt.legend(loc="lower left")
        
        plt.ylabel("HRT Schedulability")
        plt.xlabel("System Utilization")

        # plt.xticks(np.arange(0,1,0.1), step=0.2)
        # plt.yticks(np.arange(0,1,0.1), step=0.2)

        plt.savefig(file[:-4]+".pdf")
        plt.savefig(file[:-4]+".png")

if __name__ == '__main__':
    main()
