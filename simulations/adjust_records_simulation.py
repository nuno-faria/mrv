import matplotlib
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib import rcParams
import random
from collections import defaultdict
import math

rcParams['font.family'] = 'sans-serif'
rcParams['font.sans-serif'] = ['Bitstream Vera Sans Mono']

# abort rate (simulated, assuming records with enough items)
def abort_rate(records, tx_s, tx_time, n_runs=1):
    time_domain = (0, 1000 - tx_time)
    aborts = 0

    for _ in range(n_runs):
        txs_hit = defaultdict(list)
        # randomize transactions hits
        for _ in range(tx_s):
            record = random.randrange(records)
            start = random.randrange(*time_domain)
            txs_hit[record].append(start)
        
        # "abort" transactions (first commit wins)
        for record, start_times in txs_hit.items():
            times = sorted(start_times)
            for index, start_time in enumerate(times):
                # if prev transaction intersected this one
                if index > 0 and (times[index - 1] + tx_time) >= start_time:
                    aborts += 1

    return aborts / (tx_s * n_runs)


# adds or removes one record
def adjust_binary(ar, records, min_records=1, max_records=1024, ar_goal=0.1, ar_min=0.01):
    if ar > ar_goal and records < max_records:
        return records + 1
    elif ar < ar_min and records > min_records:
        return records - 1
    else:
        return records


# adds or removes a number of records based on the current number (linearly)
def adjust_linear(ar, records, min_records=1, max_records=1024, ar_goal=0.1, ar_min=0.01):
    if ar > ar_goal and records < max_records:
        return records + 1 + int(records * ar)
    elif ar < ar_min and records > min_records:
        return records - (1 + int(records * ar))
    else:
        return records


# adds or removes a number of records based on the current number (quadratically)
def adjust_quadratic(ar, records, min_records=1, max_records=1024, ar_goal=0.1, ar_min=0.01):
    if ar > ar_goal and records < max_records:
        return records + 1 + int((records * ar)**2)
    elif ar < ar_min and records > min_records:
        return records - (1 + int((records * ar)**2))
    else:
        return records


# simulate the workload
def run_simulation(time_between_adjusts, tx_s, tx_time, initial_records, adjust_records, ar_goal, ar_min, duration=150):
    adjusts_per_timestamp = defaultdict(int)
    for i in range (int(duration / (time_between_adjusts / 1000))):
        adjusts_per_timestamp[int(i * time_between_adjusts / 1000)] += 1
    
    data = defaultdict(list)
    records = initial_records
    ar = 0
    for current_time in range(duration):
        for _ in range(adjusts_per_timestamp[current_time]):
            records = adjust_records(ar, records, ar_goal=ar_goal, ar_min=ar_min)
        ar = abort_rate(records, tx_s, tx_time, n_runs=10)
        data['timestamps'].append(current_time)
        data['records'].append(records)
        data['ars'].append(ar)

        if current_time == duration / 2:
            tx_s = int(tx_s * 1.15)

    return data


def plot_adjust_simulation(data, tx_s, tx_time, adjust_delta, ar_goal, ar_min, 
                           records_color='#003773', ar_color='#ff6d59',
                           markers=['o','^','x','x','s'], file=None):
    fig, ax_ar = plt.subplots()
    ax_records = ax_ar.twinx()
    lines = []

    for i, algorithm in enumerate(data.keys()):
        l, = ax_records.plot(data[algorithm]['timestamps'], data[algorithm]['records'],
                           color=records_color, marker=markers[i], markersize=6, markerfacecolor='black',
                           markeredgecolor='black', markevery=5, linewidth=3)
        lines.append(l)
        ax_records.set_xlabel('time (s)')
        ax_records.set_ylabel('# records', color=records_color)
        ax_ar.plot(data[algorithm]['timestamps'], [x * 100 for x in data[algorithm]['ars']],
                   color=ar_color, marker=markers[i], markersize=6, markerfacecolor='black',
                   markeredgecolor='black', markevery=5, linewidth=3)
        ax_ar.set_ylabel('abort rate (%)', color=ar_color)
        ax_ar.set_yscale('log')
        ax_ar.set_yticks([100, 75, 50, 25, 10, 5, 2.5, 1])
        ax_ar.get_yaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
        ax_ar.set_xlabel('time (s)')


    plt.legend(lines, [name for name in data.keys()])
    plt.title(f'Adjust records simulations (tx_s={tx_s}, tx_time={tx_time}ms,\nadjust_delta={adjust_delta}ms, ar_goal={ar_goal}, ar_min={ar_min})')
    plt.xlabel('time (s)')

    if file:
        plt.savefig(file, dpi=300)
    else:
        plt.show()
    plt.clf()


# test adjust algorithms
def test_adjust_algorithms(tx_s, tx_time, time_between_adjusts, initial_records, ar_goal, ar_min):
    algorithms = [adjust_binary, adjust_linear, adjust_quadratic]

    data = defaultdict()
    for algorithm in algorithms:
        data[algorithm.__name__] = run_simulation(
            time_between_adjusts,tx_s, tx_time, initial_records, algorithm, ar_goal, ar_min)
    
    plot_adjust_simulation(data, tx_s, tx_time, time_between_adjusts,
                           ar_goal, ar_min, file=f'adjust_strategies_comparison.pdf')


# abort rate for some number of records
def ar_per_n_records(tx_s, tx_time, min_records=1, max_records=300):
    data = {}
    for records in range(min_records, max_records+1):
        data[records] = abort_rate(records, tx_s, tx_time, n_runs=10)
    return data


def plot_ar_per_n_records(data, color='black', file=None):
    plt.figure(figsize=(6, 4))
    plt.plot(list(reversed(list(data.values()))), list(reversed(list(data.keys()))), color=color, linewidth=2)
    plt.xlabel('abort rate')
    plt.ylabel('#records')
    plt.title('Abort rate per number of records (tx/s=1000)')
    for x, y in data.items():
        if x % 16 == 0:
            plt.annotate(round(y, 3), (y, x), size=9)
    plt.grid(linestyle='-', color='#eeeeee')

    if file:
        plt.savefig(file, dpi=300)
    else:
        plt.show()
    plt.clf()


# number of records to reach some abort rate, per number of transactions/s
def records_to_reach_ar_per_tx(ars, txs_s, tx_time, file=None, markers=['o','^','s','+','*','D','x','H']):
    data = defaultdict(lambda: defaultdict(int))
    for ar in ars:
        for tx_s in txs_s:
            records = 0
            current_ar = 1
            while current_ar > ar:
                records += 1
                current_ar = abort_rate(records, tx_s, tx_time, n_runs=10)
            data[ar][tx_s] = records
    
    plt.figure(figsize=(6,4))
    i = 0
    for ar, v in sorted(data.items()):
        tx = list(v.keys())
        records = list(v.values())
        plt.plot(tx, records, color=cm.viridis(i / (len(data) - 1)), label=str(ar * 100) + '%', linewidth=2,
                 marker=markers[i], markerfacecolor='black', markeredgecolor='black', markersize=5, markevery=5)
        i += 1

    plt.xlabel('tx/s')
    plt.ylabel('#records')
    plt.title('Records necessary to reach some AR, for some tx/s')
    plt.legend()
    plt.grid(linestyle='-', color='#f0f0f0')
    if file:
        plt.savefig(file, dpi=300)
    else:
        plt.show()
    plt.clf()


def main():
    # system variables
    time_between_adjusts = 1000 # ms
    tx_s = 1000 # expected tx/s that the row receives
    tx_time = 5 # avg duration of the transaction (ms) [0, 1000]
    initial_records = 1 # number of initial_records

    test_adjust_algorithms(tx_s, tx_time, time_between_adjusts,initial_records, 0.05, 0.01)
    plot_ar_per_n_records(ar_per_n_records(tx_s, tx_time), file='ar_per_records.pdf')
    records_to_reach_ar_per_tx([0.2, 0.1, 0.075, 0.05, 0.025, 0.01], [i * 25 for i in range(1, 41)],
                               tx_time, file='records_to_reach_ar_per_tx.pdf')


if __name__ == "__main__":
    main()
