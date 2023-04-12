# converts the dbxx1000 outputs to csv
# usage: python3 results_to_csv <resuls_folder>

import sys
import os
import re


csv = open('out.csv', 'w')
csv.write('mode,type,cc,isolation,clients,size,tx/s,ar,rt\n')

for filename in os.listdir(sys.argv[1]):
    matches = re.search(r'(\w+)-(\w+)-(\w+)-(\w+)-T(\d+)-W(\d+)', filename)
    mode = matches[1]
    name = matches[2]
    cc = matches[3]
    isolation = matches[4]
    threads = int(matches[5])
    warehouses = int(matches[6])

    with open(os.path.join(sys.argv[1], filename)) as f:
        line = f.read()
        matches = re.findall(r'(\w+)=(\d+(?:\.\d+)?)', line)
        matches_dict = {k:float(v) for (k, v) in matches}
        throughput = (matches_dict['txn_cnt'] / matches_dict['run_time']) * threads
        abort_rate = matches_dict['abort_cnt'] / (matches_dict['txn_cnt'] + matches_dict['abort_cnt'])
        latency = matches_dict['latency']
        csv.write(f'{mode},{name},{cc},{isolation},{threads},{warehouses},{throughput},{abort_rate},{latency}\n')

csv.flush()
csv.close()
