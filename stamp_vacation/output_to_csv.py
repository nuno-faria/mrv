# Converts multiple STAMP Vacation outputs to a single csv
# Usage: python3 output_to_csv.py <results-folder>

import sys
import os
import re

if len(sys.argv) != 2:
    exit('Usage: python3 output_to_csv.py <results-folder>')

out = open('out.csv', 'w')
out.write('type,clients,size,tx/s,ar,rt\n')

for file in os.listdir(sys.argv[1]):
    m = re.match(r'(\w+)_(\d+)c_(\d+)r_(\d+)n.txt', file)
    name = m[1]
    clients = int(m[2])
    relations = int(m[3])
    n_aborts = 0

    with open(os.path.join(sys.argv[1], file)) as f:
        for line in f:
            rate_match = re.search(r'mean rate = (\d+(?:\.\d+)) calls/second', line)
            if rate_match:
                tx_rate = float(rate_match[1])
                break
            
            abort_match = re.search(r'transaction aborted', line)
            if abort_match:
                n_aborts += 1

            commits_match = re.search(r'count = (\d+)', line) # commit count always appears last
            if commits_match:
                n_commits = int(commits_match[1])

    ar_rate = n_aborts / (n_commits + n_aborts)

    out.write(f'{name},{clients},{relations},{tx_rate},{ar_rate},0\n')

out.flush()
out.close()

print('Saved to out.csv')
