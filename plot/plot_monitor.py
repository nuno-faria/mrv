# Plots the number of nodes over time
# Usage: python3 plot_monitor.py <out-monitor-csv>

import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib import rcParams
import csv
from collections import defaultdict
import sys
from statistics import mean


rcParams['font.family'] = 'sans-serif'
rcParams['font.sans-serif'] = ['Roboto Mono']


if len(sys.argv) != 2:
    exit('Usage: python3 plot_monitor.py <out-monitor-csv>')

fig_size = (6, 4)
data = defaultdict(list)

with open(sys.argv[1]) as file:
    for row in csv.DictReader(file):
        data_point = (int(row['time']), int(row['nodes']) / int(row['size']))
        if 'clients' in row:
            data[row['clients']].append(data_point)
        else:
            data[row['table']].append(data_point)

plt.figure(figsize=fig_size)

i = 0
plt.title('Number of records over time')
plt.xlabel('Time (ms)')
plt.ylabel('Records per product')
plt.grid(linestyle='-', color='#f0f0f0')
for label, v in data.items():
    v = sorted(v)
    x = [t[0] for t in v]
    y = [t[1] for t in v]
    if len(data) > 1:
        color = cm.viridis(i / (len(data) - 1))
    else:
        color = cm.viridis(0)
    label += ' clients' if label.isdigit() else ''
    plt.plot(x, y, label=label, color=color)
    i += 1
plt.ylim(ymin=0)

plt.figlegend()
plt.savefig('monitor.pdf')
