# Computes the MRV conflict probability based on the number of records and the number of concurrent writes,
# using a mathematical formula.
# Outputs to stdout the csv result and saves to out_formula.pdf the respective plot.

from collections import defaultdict
from matplotlib import pyplot as plt

print('records,clients,ar')

CLIENTS = [2, 4, 8, 16, 32, 64] # numbers of clients
RECORDS = range(1, 128) # numbers of records

# run
data = defaultdict(lambda: defaultdict(list))
for w in CLIENTS:
    for n in RECORDS:
        ar = (w - (n - n * pow((1 - 1/n), w))) / w
        data[w]['X'].append(n)
        data[w]['Y'].append(ar)
        print(f'{n},{w},{ar}')

# plot
plt.figure(figsize=(5.5, 5.5))
i = 0
for clients, results in sorted(data.items()):
    plt.plot(results['X'], results['Y'], label=clients, color=plt.get_cmap('viridis')(i / (len(data) - 1) if len(data) > 1 else 1))
    i += 1

plt.legend()
plt.xlabel('Number of records')
plt.ylabel('Abort rate')
plt.title('Abort rate based on the number of records and clients')
plt.tight_layout()
plt.savefig('out_formula.pdf')
