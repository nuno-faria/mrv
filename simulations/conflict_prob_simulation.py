# Simulates the MRV conflict probability based on the number of records and the number of concurrent writes,
# to validate the mathematical formula presented in the paper.
# Outputs to stdout the csv result and saves to out_simulation.pdf the respective plot.

from collections import defaultdict
import random
from statistics import mean
from matplotlib import pyplot as plt

print('records,clients,ar')

CLIENTS = [2, 4, 8, 16, 32, 64] # numbers of clients
RECORDS = range(1, 128) # numbers of records
RUNS = 1000 # runs per test

# run
data = defaultdict(lambda: defaultdict(list))
for clients in CLIENTS:
    for records in RECORDS:
        commits = []
        for run in range(RUNS):
            bitmap = [0 for _ in range(records)]
            for c in range(clients):
                bitmap[random.randint(0, records - 1)] = 1
            commits.append(len([x for x in bitmap if x == 1]))
        ar = (clients - mean(commits)) / clients
        data[clients]['X'].append(records)
        data[clients]['Y'].append(ar)
        print(f'{records},{clients},{ar}')

# plot
plt.figure(figsize=(5.5, 5.5))
i = 0
for clients, results in sorted(data.items()):
    plt.plot(results['X'], results['Y'], label=clients, color=plt.get_cmap('viridis')(i / (len(data) - 1)))
    i += 1

plt.legend()
plt.xlabel('Number of records')
plt.ylabel('Abort rate')
plt.title('Abort rate based on the number of records and clients')
plt.tight_layout()
plt.savefig('out_simulation.pdf')
