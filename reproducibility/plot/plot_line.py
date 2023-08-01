import argparse
import matplotlib.pyplot as plt
import pandas as pd
import os

# line colors
LINES = [
    {'color': '#29066b'},
    {'color': '#1ce4bc', 'marker': 'x', 'markerfacecolor': 'none', 'markeredgecolor': 'black'},
    {'color': '#f72dd2', 'marker': 's', 'markerfacecolor': 'none', 'markeredgecolor': 'black'},
    {'color': 'black', 'linestyle': 'dotted'},
    {'color': '#d54774', 'marker': '+', 'markerfacecolor': 'none', 'markeredgecolor': 'black'},
    {'color': '#4775e7', 'marker': 'o', 'markerfacecolor': 'none', 'markeredgecolor': 'black'},
]

# args
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('file', type=str, help='File with CSV data to plot')
parser.add_argument('-o', '--output', type=str, help='Output file name', action='store', required=True)
parser.add_argument('-x', type=str, help='X axis field', action='store', required=True)
parser.add_argument('-y', type=str, help='Y axis field', action='store', required=True)
parser.add_argument('-y2', type=str, help='Secondary Y axis field', action='store', required=False)
parser.add_argument('-xname', type=str, help='X axis name', action='store', required=False)
parser.add_argument('-yname', type=str, help='Y axis name', action='store', required=False)
parser.add_argument('-y2name', type=str, help='Secondary Y axis name', action='store', required=False)
parser.add_argument('-g', '--group', type=str, help='Group lines by field', action='store', required=False)
parser.add_argument('-t', '--text', default=False, help='Whether to consider the x axis as text', action='store_true')
parser.add_argument('-m', '--mark-every', type=int, help='Marker interval', default=1, action='store')
parser.add_argument('-rx', default=False, help='Rotate x labels 90 degrees', action='store_true')
parser.add_argument('-e', '--eval', default=False, help='Treats the x and y fields as expressions (e.g., -y "field * 2", duplicates the y field)', action='store_true')
parser.add_argument('-c', '--colors', type=str, help='Order of the colors assigned to each group (groups are sorted by name) 0=blue, 1=teal, 2=pink, 3=black. E.g., 0231', action='store')
parser.add_argument('-xmin', type=int, help='X-axis minimum', action='store')
parser.add_argument('-xmax', type=int, help='X-axis maximum', action='store')
parser.add_argument('-ymin', type=int, help='Y-axis minimum', action='store')
parser.add_argument('-ymax', type=int, help='Y-axis maximum', action='store')
args = parser.parse_args()

if not os.path.isfile(args.file):
    exit(f'File {args.file} does not exist.')

# read csv
df = pd.read_csv(args.file)
if args.eval:
    df['__X'] = df.eval(args.x)
    df.sort_values(['__X'])
else:
    df = df.sort_values(args.x)

# plot
fig = plt.figure(figsize=(4, 3.5))
if args.group is not None:
    i = 0
    for label, group in df.sort_values([args.group, (args.x if not args.eval else '__X')]).groupby(args.group):
        if args.eval:
            X = group.eval(args.x) if not args.text else group.eval(args.x).astype(str)
            Y = group.eval(args.y)
        else:
            X = group[args.x] if not args.text else group[args.x].astype(str)
            Y = group[args.y]
        color = LINES[int(args.colors[i])] if args.colors is not None and i < len(args.colors) else LINES[i] if i < len(LINES) else {}
        plt.plot(X, Y, label=label, markevery=args.mark_every, **color, linewidth=2)
        if args.y2 is not None:
            Y = df.eval(args.y2) if args.eval else df[args.y2]
            plt.gca().twinx().plot(X, Y, **LINES[i], markevery=args.mark_every, 
                                   linewidth=2, label=args.y2, linestyle='dashed')
        i += 1
    plt.legend()
else:
    if args.eval:
        X = df.eval(args.x) if not args.text else df.eval(args.x).astype(str)
        Y = df.eval(args.y)
    else:
        X = df[args.x] if not args.text else df[args.x].astype(str)
        Y = df[args.y]
    plt.plot(X, Y, markevery=args.mark_every, linewidth=2, label=(args.yname or args.y),
             **(LINES[int(args.colors[0])] if args.colors is not None else LINES[0]))
    if args.y2 is not None:
        Y = df.eval(args.y2) if args.eval else df[args.y2]
        plt.gca().twinx().plot(X, Y, markevery=args.mark_every, linewidth=2, label=(args.y2name or args.y2),
                               **(LINES[int(args.colors[1])] if args.colors is not None else LINES[2]))
        lines = ([y for x in plt.gcf().get_axes() for y in x.get_legend_handles_labels()[0]])
        labels = ([y for x in plt.gcf().get_axes() for y in x.get_legend_handles_labels()[1]])
        plt.legend(lines, labels)

# axis labels
ax1 = plt.gcf().get_axes()[0]
ax1.set_ylim(0)
ax1.set_xlabel(args.xname or args.x)
ax1.set_ylabel(args.yname or args.y)
if args.y2 is not None:
    ax2 = plt.gcf().get_axes()[1]
    ax2.set_ylim(0)
    ax2.set_ylabel(args.y2name or args.y2)
if args.rx:
    plt.xticks(rotation=90)

# axis min/max
if args.xmin:
    plt.xlim(left=args.xmin)
if args.xmax:
    plt.xlim(right=args.xmax)
if args.ymin:
    plt.ylim(left=args.ymin)
if args.ymax:
    plt.ylim(right=args.ymax)

plt.tight_layout()
plt.savefig(args.output)
