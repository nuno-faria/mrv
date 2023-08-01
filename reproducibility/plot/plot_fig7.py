import seaborn as sns
import pandas as pd
import sys
import matplotlib.pyplot as plt
from matplotlib.colors import TwoSlopeNorm, LinearSegmentedColormap
import os


def genColormap(reversed=False):
    colors = [(247,45,209), (255, 255, 255), (41,6,107)]
    cmap = LinearSegmentedColormap.from_list("", [[x/255 for x in c] for c in colors])
    if reversed:
        return cmap.reversed()
    else:
        return cmap


if len(sys.argv) < 3:
    exit('Missing filename with CSV data and output name.')
    
if not os.path.isfile(sys.argv[1]):
    exit(f'File {sys.argv[1]} does not exist.')

df = pd.read_csv(sys.argv[1])
nKs = len(set(df['balanceMinmaxK']))
nRecords = len(set(df['initialNodes']))
baseZeros = df[df['balanceMinmaxK'] == 1]['zeros']
baseZeros = baseZeros.loc[baseZeros.index.repeat(nKs)].reset_index(drop=True)
baseTime = df[df['balanceMinmaxK'] == 1]['balance_time']
baseTime = baseTime.loc[baseTime.index.repeat(nKs)].reset_index(drop=True)
df['ratio'] = (df['zeros'] / baseZeros) * \
    ((df['balance_time'] + 100) / (baseTime + 100))
df.loc[df['balanceMinmaxK'] * 2 > df['initialNodes'], 'ratio'] = float('NaN')
dfPivoted = df.pivot(index='initialNodes', columns='balanceMinmaxK', values='ratio')
dfPivotedNormalized = dfPivoted.sub(dfPivoted.min(axis=1), axis=0).div(dfPivoted.max(axis=1) - dfPivoted.min(axis=1), axis=0)

vmax = 1
vcenter = 0.5
vmin = 0
divnorm = TwoSlopeNorm(vmin=vmin, vcenter=0.5, vmax=vmax)
fig = plt.figure(figsize=(nKs / 1.5, nRecords / 2.5))
ax = sns.heatmap(dfPivotedNormalized, cmap=genColormap(reversed=True), vmin=vmin, vmax=vmax,
                 norm=divnorm, annot=dfPivoted, fmt='.2f', linewidths=0.5,
                 cbar=False)
plt.yticks(rotation=0)
plt.ylabel("Records per product")
plt.xlabel("K")
plt.tight_layout()
ax.cmap = False
plt.savefig(sys.argv[2])
