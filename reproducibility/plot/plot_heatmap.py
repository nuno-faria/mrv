import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib.colors import TwoSlopeNorm, LinearSegmentedColormap
import sys
import seaborn as sns
import os


def genColormap(reversed=False):
    colors = [(247,45,209), (255, 255, 255), (41,6,107)]
    cmap = LinearSegmentedColormap.from_list("", [[x/255 for x in c] for c in colors])
    if reversed:
        return cmap.reversed()
    else:
        return cmap

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('file', type=str, help='File with CSV data to plot')
parser.add_argument('-o', '--output', type=str, help='Output file name', action='store', required=True)
parser.add_argument('-b', '--base', type=str, help='Filter with the base data', action='store', required=True)
parser.add_argument('-n', '--new', type=str, help='Filter with the new data', action='store', required=True)
parser.add_argument('-x', type=str, help='X axis field', action='store', required=True)
parser.add_argument('-y', type=str, help='Y axis field', action='store', required=True)
parser.add_argument('-v', type=str, help='Value field', action='store', required=True)
parser.add_argument('-xname', type=str, help='X axis name', action='store', required=False)
parser.add_argument('-yname', type=str, help='Y axis name', action='store', required=False)
parser.add_argument('-l', '--label', type=str, help='Colorbar label', action='store', required=False)
args = parser.parse_args()

if not os.path.isfile(args.file):
    exit(f'File {args.file} does not exist.')

rcParams['font.size'] = 7

# read and process data
df = pd.read_csv(sys.argv[1])
df[args.v] = df[args.v].astype(int)
base = df[df.eval(args.base)]
base_pivoted = base.pivot(index=args.y, columns=args.x, values=args.v)
base_pivoted = base_pivoted.sort_index(ascending=False)
new = df[df.eval(args.new)]
new_pivoted = new.pivot(index=args.y, columns=args.x, values=args.v)
new_pivoted = new_pivoted.sort_index(ascending=False)
n_clients = len(base_pivoted.columns)
size = len(base_pivoted.index)

# relative tx/s results
fig = plt.figure(figsize=(n_clients / 2.3, size / 3.7))
diff = new_pivoted / base_pivoted
diff = diff.astype(float)
vmax = 2
divnorm = TwoSlopeNorm(vmin=0, vcenter=1, vmax=vmax)
ax = sns.heatmap(diff, cmap=genColormap(), vmin=0, vmax=vmax, norm=divnorm, annot=True, fmt='.1f',
                 annot_kws={"size": 7}, linewidths=0.5, cbar_kws={'label': args.label or 'Diff', 'ticks': [0, 0.5, 1, 1.5, 2]})


plt.yticks(rotation=0)
plt.ylabel(args.yname or args.y)
plt.xlabel(args.xname or args.x)
plt.tight_layout()
ax.cmap = False
plt.savefig(args.output)
