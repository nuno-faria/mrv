# Converts multiple tpcc outputs into a single csv
# Usage: python3 output_to_csv.py <results-folder>

import sys
import os
import re
from collections import defaultdict

if len(sys.argv) != 2:
    exit('Usage: python3 output_to_csv.py <results-folder>')

data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

for file in os.listdir(sys.argv[1]):
    m = re.match(r'(\w+)_(\d+)_(\d+).txt', file)
    name = m[1]
    warehouses = m[2]
    clients = m[3]

    with open(os.path.join(sys.argv[1], file)) as f:
        for line in f:
            tx_match = re.search(r'transactions:.*?\((\d+(\.\d+)?) per sec.\)', line)
            if tx_match:
                data[name][warehouses][clients]['tx'] = float(tx_match[1])
            
            aborts_match = re.search(r'ignored errors:.*?\((\d+(\.\d+)?) per sec.\)', line)
            if aborts_match:
                data[name][warehouses][clients]['aborts'] = float(aborts_match[1])
            
            rt_match = re.search(r'avg:\s+(\d+\.\d+)', line)
            if rt_match:
                data[name][warehouses][clients]['rt'] = float(rt_match[1])

out = open('out.csv', 'w')
out.write('type,clients,size,tx/s,ar,rt\n')

for name, v1 in data.items():
    for warehouses, v2 in v1.items():
        for clients, v3 in v2.items():
            out.write(f"{name},{clients},{warehouses},{v3['tx']},{round(v3['aborts'] / (v3['aborts'] + v3['tx']), 2)},{v3['rt']}\n")

out.flush()
out.close()

print('Saved to out.csv')
