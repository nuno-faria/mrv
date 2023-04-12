# Plots the absolute and relatives (mrv-to-normal ratio) heatmaps
# Usage: python3 plot_heatmap.py <results_csv>

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib.colors import TwoSlopeNorm, LinearSegmentedColormap, ListedColormap
import sys
import numpy as np
import seaborn as sns


def genColormap(reversed=False):
    colors = [(247,45,209), (255, 255, 255), (41,6,107)]
    cmap = LinearSegmentedColormap.from_list("", [[x/255 for x in c] for c in colors])
    if reversed:
        return cmap.reversed()
    else:
        return cmap


rcParams['font.size'] = 7

# read and process data
df = pd.read_csv(sys.argv[1])
df['tx/s'] = df['tx/s'].astype(int)
normal = df[df['type'] == 'normal']
normal_pivoted = normal.pivot(index='size', columns='clients', values='tx/s')
normal_pivoted = normal_pivoted.sort_index(ascending=False)
mrv = df[df['type'] == 'mrv']
mrv_pivoted = mrv.pivot(index='size', columns='clients', values='tx/s')
mrv_pivoted = mrv_pivoted.sort_index(ascending=False)
n_clients = len(normal_pivoted.columns)
size = len(normal_pivoted.index)


# absolute tx/s results
vmin = min(normal_pivoted.values.min(), mrv_pivoted.values.min())
vmax = max(normal_pivoted.values.max(), mrv_pivoted.values.max())
fig, axs = plt.subplots(ncols=3, gridspec_kw=dict(width_ratios=[1, 1, 0.05]), 
                        figsize=(n_clients * 2 / 1.8, size / 1.8))
sns.heatmap(normal_pivoted, cbar=False, ax=axs[0], vmin=0, vmax=vmax, cmap='RdPu', annot=True, fmt='d',
            annot_kws={"size":7}, linewidths=0.5)
sns.heatmap(mrv_pivoted, cbar=False, ax=axs[1], vmin=0, vmax=vmax, cmap='RdPu', annot=True, fmt='d',
            annot_kws={"size":7}, linewidths=0.5)
fig.colorbar(axs[1].collections[0], cax=axs[2], label='transactions / s', cmap='RdPu')
axs[0].set_title('native')
axs[1].set_title('mrv')
plt.suptitle('Transaction rate')
fig.subplots_adjust(top=0.88, bottom=0.15, wspace=0.3)
plt.savefig('absolute_txs.pdf', dpi=300)
plt.clf()


# relative tx/s results
fig = plt.figure(figsize=(n_clients / 2, size / 3.7))
diff = mrv_pivoted / normal_pivoted
diff = diff.astype(float)
vmax = 2 #if diff.max().max() < 8 else 10
divnorm = TwoSlopeNorm(vmin=0, vcenter=1, vmax=vmax)
ax = sns.heatmap(diff, cmap=genColormap(), vmin=0, vmax=vmax, norm=divnorm, annot=True, fmt='.1f',
                 annot_kws={"size": 7}, linewidths=0.5, cbar_kws={'label': 'Relative throughput', 'ticks': [0, 0.5, 1, 1.5, 2]})

plt.title('Relative transaction rate')
plt.yticks(rotation=0)
plt.ylabel("Number of rows")
plt.xlabel("Clients")
plt.tight_layout()
ax.cmap = False
plt.savefig('relative_txs.pdf', dpi=300)


# relative rt results
normal_pivoted = normal.pivot(index='size', columns='clients', values='rt')
normal_pivoted = normal_pivoted.sort_index(ascending=False)
mrv_pivoted = mrv.pivot(index='size', columns='clients', values='rt')
mrv_pivoted = mrv_pivoted.sort_index(ascending=False)

fig = plt.figure(figsize=(n_clients / 2, size / 3.7))
diff = mrv_pivoted / normal_pivoted
diff = diff.replace([np.inf, -np.inf], np.nan)
diff = diff.fillna(1)
diff = diff.astype(float)
ax = sns.heatmap(diff, cmap=genColormap(reversed=True), center=1, vmin=0, vmax=2, annot=True, fmt='.2f',
                 annot_kws={"size": 7}, linewidths=0.5, cbar_kws={'label': 'Relative response time'})

plt.title('Relative response time')
plt.ylabel("Number of rows")
plt.xlabel("Clients")
plt.tight_layout()
plt.savefig('relative_rt.pdf', dpi=300)


# relative ar results
normal_pivoted = normal.pivot(index='size', columns='clients', values='ar')
normal_pivoted = normal_pivoted.sort_index(ascending=False)
mrv_pivoted = mrv.pivot(index='size', columns='clients', values='ar')
mrv_pivoted = mrv_pivoted.sort_index(ascending=False)

fig = plt.figure(figsize=(n_clients / 2, size / 3.7))
diff = mrv_pivoted / normal_pivoted
diff = diff.replace([np.inf, -np.inf], np.nan)
diff = diff.fillna(1)
diff = diff.astype(float)
ax = sns.heatmap(diff, center=1, vmin=0, vmax=2, cmap=genColormap(reversed=True), annot=True, fmt='.2f',
                 annot_kws={"size": 7}, linewidths=0.5, cbar_kws={'label': 'Relative abort rate'})

plt.title('Relative abort rate')
plt.ylabel("Number of rows")
plt.xlabel("Clients")
plt.tight_layout()
plt.savefig('relative_ar.pdf', dpi=300)
