# Plots the throughput and the abort for each number of clients and benchmark type 
# Usage: python3 plot_bars.py <results-csv> 

import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib import rcParams
import csv
from collections import defaultdict
import sys
import pandas as pd
import re


rcParams['font.family'] = 'sans-serif'
rcParams['font.sans-serif'] = ['Roboto Mono']

colors = ['#210c4a', '#ff8c00']


def custom_sort_key(s):
    if s == 'native':
        return ['0']
    else:
        return [int(x) if x.isdigit() else x for x in re.split(r'\d+', s)]


def human_format(num, round=True):
    num = float('{:.3g}'.format(num))
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    if round:
        num = int(num)
    return '{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T'][magnitude])


if len(sys.argv) != 2:
    exit("Usage: python3 plot_bars.py <results-csv>") 


data = defaultdict(lambda: defaultdict(
    lambda: defaultdict(lambda: defaultdict(tuple))))

types_ = set()
with open(sys.argv[1]) as file:
    for row in csv.DictReader(file):
        size = int(row['size'])
        type_ = 'native' if row['type'] == 'normal' else row['type']
        clients = int(row['clients'])
        tx_s = float(row['tx/s'])
        ar = round(float(row['ar']), 2)
        rt = round(float(row['rt']), 2) if 'rt' in row else 0
        data[size][type_][clients] = (tx_s, ar, rt)
        types_.add(type_)


if len(types_) > len(colors):
    colors = [cm.inferno(i / (len(types_) - 1)) for i in range(len(types_))]

for size, v in data.items():
    pandas_data = []
    for type_, v2 in v.items():
        for clients, (tx_s, ar, rt) in sorted(v2.items()):
            pandas_data.append([type_, clients, tx_s, ar, rt])
    df = pd.DataFrame(pandas_data, columns=['type', 'clients', 'tx_s', 'ar', 'rt'])
    df["type"] = pd.Categorical(df['type'], sorted(v.keys(), key=custom_sort_key))
    df = df.sort_values("type")
    df_tx = df.pivot('clients', 'type', 'tx_s')
    df_ar = df.pivot('clients', 'type', 'ar')
    df_rt = df.pivot('clients', 'type', 'rt')

    # tx/s
    df_tx.plot(kind='bar', color=colors, zorder=2)
    plt.ylabel('tx/s')
    plt.grid(linestyle='-', color='#f0f0f0', zorder=-1, axis='y')
    plt.title('Average thoughput')
    plt.savefig(f'results_txs_{size}.pdf', dpi=300, bbox_inches='tight')
    plt.close()
    
    # abort rate
    df_ar.plot(kind='bar', color=colors, zorder=2)
    plt.ylabel('ar')
    plt.grid(linestyle='-', color='#f0f0f0', zorder=-1, axis='y')
    plt.title('Average abort rate')
    plt.savefig(f'results_ar_{size}.pdf', dpi=300, bbox_inches='tight')
    plt.close()

    # response time
    df_rt.plot(kind='bar', color=colors, zorder=2)
    plt.ylabel('response time (ms)')
    plt.grid(linestyle='-', color='#f0f0f0', zorder=-1, axis='y')
    plt.title('Average response time')
    plt.savefig(f'results_rt_{size}.pdf', dpi=300, bbox_inches='tight')
    plt.close()
