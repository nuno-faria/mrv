#!/bin/bash

mkdir -p plots

if [[ $# -eq 0 || " ${@} " =~ " fig4 " ]]; then
    echo "Plotting fig4"
    mkdir -p plots/fig4
    python3 plot/plot_fig4.py results/fig4/write_postgres.csv plots/fig4/write_postgres.pdf
    python3 plot/plot_fig4.py results/fig4/read_postgres.csv plots/fig4/read_postgres.pdf
    python3 plot/plot_fig4.py results/fig4/write_mongodb.csv plots/fig4/write_mongodb.pdf
    python3 plot/plot_fig4.py results/fig4/read_mongodb.csv plots/fig4/read_mongodb.pdf
fi

if [[ $# -eq 0 || " ${@} " =~ " fig5 " ]]; then
    echo "Plotting fig5"
    mkdir -p plots/fig5
    python3 plot/plot_line.py results/fig5/results.csv -x "time / 1000" -xname "Time (s)" -y nodes -yname "Total number of records" -g loadIncrease -m 5 -c 2103 -e -o plots/fig5/records.pdf
    python3 plot/plot_line.py results/fig5/results.csv -x "time / 1000" -xname "Time (s)" -y ar -yname "Abort rate" -g loadIncrease -m 5 -c 2103 -e -o plots/fig5/ar.pdf
fi

if [[ $# -eq 0 || " ${@} " =~ " fig6 " ]]; then
    echo "Plotting fig6"
    mkdir -p plots/fig6
    python3 plot/plot_line.py results/fig6/results.csv -x initialNodes -xname "Records per product" -y balance_time -yname "Balance time (ms)" -g balanceAlgorithm -t -rx -c 0132 -o plots/fig6/balanceTime.pdf
    python3 plot/plot_line.py results/fig6/results.csv -x initialNodes -xname "Records per product" -y "zeros/initialNodes * 100" -yname "Percentage of zeros" -g balanceAlgorithm -t -rx -c 0132 -e -o plots/fig6/zeros.pdf
    python3 plot/plot_line.py results/fig6/results.csv -x initialNodes -xname "Records per product" -y tx/s -yname "Throughput (tx/s)" -g balanceAlgorithm -t -rx -c 0132 -o plots/fig6/tx.pdf
    python3 plot/plot_line.py results/fig6/results.csv -x initialNodes -xname "Records per product" -y ar -yname "Abort Rate" -g balanceAlgorithm -t -rx -c 0132 -o plots/fig6/ar.pdf
fi

if [[ $# -eq 0 || " ${@} " =~ " fig7 " ]]; then
    echo "Plotting fig7"
    mkdir -p plots/fig7
    python3 plot/plot_fig7.py results/fig7/results.csv plots/fig7/ratio.pdf
fi

if [[ $# -eq 0 || " ${@} " =~ " fig8 " ]]; then
    echo "Plotting fig8"
    mkdir -p plots/fig8
    python3 plot/plot_line.py results/fig8/balance.csv -x balanceWindow -xname "Balance window (%)" -y ar -yname "Abort rate" -y2 "balance_time/1000" -y2name "Balance time (s)" -c 20 -e -o plots/fig8/balance.pdf
    python3 plot/plot_line.py results/fig8/adjust.csv -x adjustWindow -xname "Adjust window (%)" -y ar -yname "Abort rate" -y2 "adjust_time/1000" -y2name "Adjust time (s)" -c 20 -e -o plots/fig8/adjust.pdf
fi

if [[ $# -eq 0 || " ${@} " =~ " fig9 " ]]; then
    echo "Plotting fig9"
    mkdir -p plots/fig9
    python3 plot/plot_line.py results/fig9/results.csv -x "readRatio * 100" -xname "% of read transactions" -y '`txWrite/s`' -yname "Write throughput (tx/s)" -g type -c 1023 -e -o plots/fig9/writes.pdf
    python3 plot/plot_line.py results/fig9/results.csv -x "readRatio * 100" -xname "% of read transactions" -xmin 50 -y '`txRead/s`' -yname "Read throughput (tx/s)" -g type -c 1023 -e -o plots/fig9/reads.pdf
fi

if [[ $# -eq 0 || " ${@} " =~ " fig10 " ]]; then
    echo "Plotting fig10"
    mkdir -p plots/fig10
    python3 plot/plot_line.py results/fig10/results.csv -x "unevenScale" -xname "Uneven scale" -y "tx/s" -yname "Throughput (tx/s)" -g type -c 1023 -o plots/fig10/tx.pdf
    python3 plot/plot_line.py results/fig10/results.csv -x "unevenScale" -xname "Uneven scale" -y "ar" -yname "Abort rate" -g type -c 1023 -o plots/fig10/ar.pdf
fi

if [[ $# -eq 0 || " ${@} " =~ " fig11 " ]]; then
    echo "Plotting fig11"
    mkdir -p plots/fig11
    python3 plot/plot_line.py results/fig11/results.csv -x "size" -xname "Warehouses" -y "tx/s" -yname "Throughput (tx/s)" -g type -c 210 -o plots/fig11/tx.pdf
    python3 plot/plot_line.py results/fig11/results.csv -x "size" -xname "Warehouses" -y "ar" -yname "Abort rate" -g type -c 210 -o plots/fig11/ar.pdf
fi

if [[ $# -eq 0 || " ${@} " =~ " fig12 " ]]; then
    echo "Plotting fig12"
    mkdir -p plots/fig12
    python3 plot/plot_heatmap.py results/fig12/microbench.csv -b 'type == "normal"' -n 'type == "mrv"' -x clients -y size -v 'tx/s' -xname Clients -yname Products -l "Relative throughput" -o plots/fig12/microbench.pdf
    python3 plot/plot_heatmap.py results/fig12/tpcc.csv -b 'type == "normal"' -n 'type == "mrv"' -x clients -y size -v 'tx/s' -xname Clients -yname Warehouses -l "Relative throughput" -o plots/fig12/tpcc.pdf
    python3 plot/plot_heatmap.py results/fig12/vacation.csv -b 'type == "normal"' -n 'type == "mrv"' -x clients -y size -v 'tx/s' -xname Clients -yname "Items per type" -l "Relative throughput" -o plots/fig12/vacation.pdf
fi

if [[ $# -eq 0 || " ${@} " =~ " fig13 " ]]; then
    echo "Plotting fig13"
    mkdir -p plots/fig13
    python3 plot/plot_heatmap.py results/fig13/postgres.csv -b 'type == "normal"' -n 'type == "mrv"' -x clients -y size -v 'tx/s' -xname Clients -yname Products -l "Relative throughput" -o plots/fig13/postgres.pdf
    python3 plot/plot_heatmap.py results/fig13/mongodb.csv -b 'type == "normal"' -n 'type == "mrv"' -x clients -y size -v 'tx/s' -xname Clients -yname Products -l "Relative throughput" -o plots/fig13/mongodb.pdf
    python3 plot/plot_heatmap.py results/fig13/mysql.csv -b 'type == "normal"' -n 'type == "mrv"' -x clients -y size -v 'tx/s' -xname Clients -yname Products -l "Relative throughput" -o plots/fig13/mysql.pdf
fi

if [[ $# -eq 0 || " ${@} " =~ " fig14 " ]]; then
    echo "Plotting fig14"
    mkdir -p plots/fig14
    python3 plot/plot_line.py results/fig14/without_mrvs.csv -x "size" -xname "Warehouses" -y "tx/s" -yname "Throughput (tx/s)" -t -g cc -c 135204 -o plots/fig14/without_mrvs.pdf
    python3 plot/plot_line.py results/fig14/with_mrvs.csv -x "size" -xname "Warehouses" -y "tx/s" -yname "Throughput (tx/s)" -t -g cc -c 15204 -o plots/fig14/with_mrvs.pdf
fi

if [[ $# -eq 0 || " ${@} " =~ " tab4 " ]]; then
    echo "Plotting tab4"
    mkdir -p plots/tab4
    python3 plot/plot_tab4.py results/tab4/results.csv plots/tab4/results.tex
    pdflatex -interaction=nonstopmode -output-directory plots/tab4 plots/tab4/results.tex > /dev/null
    mv plots/tab4/results.pdf plots/tab4/r.pdf
    rm plots/tab4/results*
    mv plots/tab4/r.pdf plots/tab4/results.pdf
fi
