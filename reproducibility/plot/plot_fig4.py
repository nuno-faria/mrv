import sys
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter
import pandas as pd
import os

if len(sys.argv) < 3:
    exit('Missing filename with CSV data and output name.')
    
if not os.path.isfile(sys.argv[1]):
    exit(f'File {sys.argv[1]} does not exist.')

df = pd.read_csv(sys.argv[1])
plt.figure(figsize=(2.5, 2.5))

if df[df['type'] == 'normal']['rt_add'][0] != 0:
    base_add = df[df['type'] == 'normal']['rt_add'][0]
    base_sub = df[df['type'] == 'normal']['rt_sub'][0]
    df['add_ratio'] = df['rt_add'] / base_add
    df['sub_ratio'] = df['rt_sub'] / base_sub
    mrv = df[df['type'] == 'mrv']
    plt.plot(mrv['initialNodes'].astype(str), mrv['add_ratio'], label='add', color='#29066b')
    plt.plot(mrv['initialNodes'].astype(str), mrv['sub_ratio'], label='sub', color='#f72dd2', 
             marker='s', markerfacecolor='none', markeredgecolor='black')
else:
    base_read = df[df['type'] == 'normal']['rt'][0]
    df['read_ratio'] = df['rt'] / base_read
    mrv = df[df['type'] == 'mrv']
    plt.plot(mrv['initialNodes'].astype(str), mrv['read_ratio'], label='read', color='#29066b')

plt.legend()
plt.yscale('log')
plt.gca().yaxis.set_major_formatter(StrMethodFormatter('{x:.0f}'))
plt.ylim(1, 50)
plt.xlabel('MRV size')
plt.ylabel('Response time ratio')
plt.xticks(rotation=90)
plt.tight_layout()
plt.savefig(sys.argv[2])
