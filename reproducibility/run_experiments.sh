#!/bin/bash

# runtime for each test (seconds)
TIME=60
# cooldown between each test (seconds)
COOLDOWN=5


# recovers the original configs
_recover_confs () {
    cp ../microbench/src/main/resources/config.yaml.bak ../microbench/src/main/resources/config.yaml
    cp ../DBx1000/run.sh.bak ../DBx1000/run.sh
    cp ../tpcc/run.sh.bak ../tpcc/run.sh
    cp ../stamp_vacation/run.sh.bak ../stamp_vacation/run.sh
}


# starts or removes a mongodb or mysqlgr cluster
# (containers are started and removed for their specific tests to reduce memory usage)
_setup_cluster () {
    if [[ $1 == "mongodb" ]]; then
        pushd . > /dev/null
        cd ../deploy/mongodb
        if [[ $2 == "start" ]]; then
            ./deploy_cluster.sh
        else
            ./remove_cluster.sh
        fi
        popd > /dev/null
    elif [[ $1 == "mysql" ]]; then
        pushd . > /dev/null
        cd ../deploy/mysqlgr
        if [[ $2 == "start" ]]; then
            ./deploy_cluster.sh
        else
            ./remove_cluster.sh
        fi
        popd > /dev/null
    elif [[ ! $1 == "postgres" ]]; then
        echo "Invalid database."
        exit 1
    fi
}


# clean previous raw results
_clean () {
    rm -f ../microbench/*.csv
    rm -rf ../tpcc/results
    rm -rf ../stamp_vacation/results
    rm -f ../generic/mrvgenericworker/*.csv
}


# initialization commands
_init () {
    # set runtime and cooldown
    sed -i "s/time:.*/time: $(($TIME + 5))/" ../microbench/src/main/resources/config.yaml
    sed -i "s/TIME=.*/TIME=$TIME/" ../tpcc/run.sh
    sed -i "s/TIME=.*/TIME=$TIME/" ../stamp_vacation/run.sh
    sed -i "s/cooldown:.*/cooldown: $COOLDOWN/" ../microbench/src/main/resources/config.yaml
    sed -i "s/sleep.*# cooldown/sleep $COOLDOWN # cooldown/" ../tpcc/run.sh
    sed -i "s/sleep.*# cooldown/sleep $COOLDOWN # cooldown/" ../stamp_vacation/run.sh
    sed -i "s/sleep.*# cooldown/sleep $COOLDOWN # cooldown/" ../DBx1000/run.sh

    # remove previous results
    _clean
}


# updates a configuration parameter
# (must be in the respective benchmark directory)
_set() {
    if [[ $# -ne 3 ]]; then
        echo "Invalid arguments. Usage: _set (micro|tpcc|vacation|dbx1000) key value"
        exit 1
    fi

    if ! [[ $1 == +(micro|tpcc|vacation|dbx1000) ]]; then
        echo "Invalid benchmark: must be 'micro', 'tpcc', 'vacation', or 'dbx1000'"
        exit 1
    fi

    if [[ $1 == "micro" ]]; then
        if ! grep -q "$2:" src/main/resources/config.yaml; then
            echo "Config not found: $2"
            exit 1
        fi
        sed -i "s/$2:.*/$2: $3/" src/main/resources/config.yaml
    else
        if ! grep -q "$2=" run.sh; then
            echo "Config not found: $2"
            exit 1
        fi
        sed -i "s/$2=.*/$2=$3/" run.sh
    fi
}


# sets the database connection variables in the microbenchmark
_setMicrobenchDb () {
    if [[ $1 == "postgres" ]]; then
        _set micro dbms "postgresql"
        _set micro servers "[127.0.0.1:5432]"
        _set micro user "postgres"
        _set micro pass "postgres"
    elif [[ $1 == "mongodb" ]]; then
        _set micro dbms "mongodb"
        _set micro servers "[127.0.0.1:27011, 127.0.0.1:27012, 127.0.0.1:27013]"
        _set micro user "null"
        _set micro pass "null"
    elif [[ $1 == "mysql" ]]; then
        _set micro dbms "mysql"
        _set micro servers "[127.0.0.1:3307, 127.0.0.1:3308, 127.0.0.1:3309]"
        _set micro user "root"
        _set micro pass "root"
    else
        echo "Invalid database."
        exit 1
    fi
}


fig4 () {
    echo "Running ${FUNCNAME[0]} $*"
    if ! [[ $1 == +(postgres|mongodb) ]]; then
        echo "${FUNCNAME[0]}: Invalid arguments: must be 'postgres' or 'mongodb'"
        exit 1
    fi

    mkdir -p results/fig4
    cd ../microbench

    # database
    _setMicrobenchDb $1
    _setup_cluster $1 start

    # shared confs
    _set micro clients "[1]"
    _set micro initialNodes "[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]"
    _set micro sizes "[1]"
    _set micro types "[normal, mrv]"
    _set micro workers "balance"

    # write test
    if [[ -z $2 || $2 == "write" ]]; then
        _set micro mode "write"
        rm -f *.csv
        mvn exec:java -Dexec.mainClass="Main"
        rm *monitor*
        mv *.csv ../reproducibility/results/fig4/write_$1.csv
    fi

    # read test
    if [[ -z $2 || $2 == "read" ]]; then
        _set micro mode "read"
        mvn exec:java -Dexec.mainClass="Main"
        rm *monitor*
        mv *.csv ../reproducibility/results/fig4/read_$1.csv
    fi

    _setup_cluster $1 remove
}


fig5 () {
    echo "Running ${FUNCNAME[0]} $*"
    mkdir -p results/fig5
    cd ../microbench

    # database
    _setMicrobenchDb postgres

    # shared confs
    _set micro time "60"
    _set micro mode "increasedLoad"
    _set micro loadIncreases "[2, 3, 4]"
    _set micro clients "[8]"
    _set micro sizes "[256]"
    _set micro types "[mrv]"
    _set micro initialNodes "[1]"
    _set micro monitorDelta "1000"
    _set micro adjustDeltas "[1000]"

    # dynamic
    mvn exec:java -Dexec.mainClass="Main"
    sed -i "s/\(linear,[[:digit:]]\+\),/\1x,/g" *monitor*.csv
    sed -i "/0.0$/d" *monitor*.csv # remove abort rate measurements made when the status table was cleared
    mv *monitor*.csv ../reproducibility/results/fig5/results.csv
    rm *.csv

    # static
    _set micro loadIncreases "[4]"
    _set micro adjustWindows "[0]"
    mvn exec:java -Dexec.mainClass="Main"
    sed -i "s/\(linear,[[:digit:]]\+\),/\1x static,/g" *monitor*.csv
    sed -i "/0.0$/d" *monitor*.csv # remove abort rate measurements made when the status table was cleared
    tail -n+2 *monitor*.csv >> ../reproducibility/results/fig5/results.csv
    rm *.csv
}


fig6 () {
    echo "Running ${FUNCNAME[0]} $*"
    mkdir -p results/fig6
    cd ../microbench

    # database
    _setMicrobenchDb postgres

    # run
    _set micro opDistribution "uneven"
    _set micro unevenScales "[1000]"
    _set micro clients "[64]"
    _set micro sizes "[64]"
    _set micro initialStocks "[1]"
    _set micro zeroNodesPercentages "[100]"
    _set micro types "[mrv]"
    _set micro initialNodes "[2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]"
    _set micro workers "balance"
    _set micro balanceMinmaxKs "[1]"
    _set micro balanceMinmaxKRatios "[0]"
    _set micro balanceMinDiffs "[10]"
    _set micro balanceAlgorithms "[all, minmax, random, none]"
    _set micro balanceWindows "[100000]"
    mvn exec:java -Dexec.mainClass="Main"
    rm *monitor*.csv
    mv *.csv ../reproducibility/results/fig6/results.csv
}


fig7 () {
    echo "Running ${FUNCNAME[0]} $*"
    mkdir -p results/fig7
    cd ../microbench

    # database
    _setMicrobenchDb postgres

    # run
    _set micro opDistribution "uneven"
    _set micro unevenScales "[1000]"
    _set micro clients "[64]"
    _set micro sizes "[64]"
    _set micro initialStocks "[2000]"
    _set micro zeroNodesPercentages "[100]"
    _set micro types "[mrv]"
    _set micro initialNodes "[4, 8, 16, 32, 64, 128, 256]"
    _set micro workers "balance"
    _set micro balanceMinmaxKs "[1, 2, 4, 8, 16, 32, 64, 128]"
    _set micro balanceMinmaxKRatios "[0]"
    _set micro balanceMinDiffs "[0]"
    _set micro balanceAlgorithms "[minmax]"
    _set micro balanceWindows "[100000]"
    mvn exec:java -Dexec.mainClass="Main"
    rm *monitor*.csv
    mv *.csv ../reproducibility/results/fig7/results.csv
}


fig8 () {
    echo "Running ${FUNCNAME[0]} $*"
    mkdir -p results/fig8
    cd ../microbench

    # database
    _setMicrobenchDb postgres

    # shared confs
    _set micro clients "[64]"
    _set micro sizes "[100000]"
    _set micro accessDistribution "powerlaw"
    _set micro types "[mrv]"
    _set micro initialStocks "[100000000]"

    # balance worker
    if [[ -z $1 || $1 == "balance" ]]; then
        _set micro opDistribution "uneven"
        _set micro unevenScales "[100000000]"
        _set micro initialNodes "[64]"
        _set micro workers "balance"
        _set micro zeroNodesPercentages "[100]"
        _set micro balanceWindows "[0, 20, 50, 70, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]"
        _set micro balanceMinDiffs "[0]"
        _set micro balanceMinmaxKs "[1]"
        _set micro balanceMinmaxKRatios "[0]"
        mvn exec:java -Dexec.mainClass="Main"
        rm *monitor*.csv
        mv *.csv ../reproducibility/results/fig8/balance.csv
    fi

    # adjust worker
    if [[ -z $1 || $1 == "adjust" ]]; then
        _set micro opDistribution "uniform"
        _set micro initialNodes "[1]"
        _set micro workers "all"
        _set micro zeroNodesPercentages "[0]"
        _set micro balanceWindows "[1000]"
        _set micro adjustWindows "[0, 2, 5, 7, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]"
        mvn exec:java -Dexec.mainClass="Main"
        rm *monitor*.csv
        mv *.csv ../reproducibility/results/fig8/adjust.csv
    fi
}


fig9 () {
    echo "Running ${FUNCNAME[0]} $*"
    mkdir -p results/fig9
    cd ../microbench

    # database
    _setMicrobenchDb postgres

    # shared confs
    _set micro clients "[32]"
    _set micro sizes "[32]"
    _set micro mode "hybrid"
    _set micro hybridReadRatios "[0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1]"

    # normal, mrv, phaseReconciliation
    _set micro types "[normal, mrv, phaseReconciliation]"
    _set micro initialNodes "[1]"
    mvn exec:java -Dexec.mainClass="Main"
    rm *monitor*.csv
    mv *.csv ../reproducibility/results/fig9/results.csv

    # static
    _set micro types "[mrv]"
    _set micro initialNodes "[0]"
    _set micro workers "none"
    mvn exec:java -Dexec.mainClass="Main"
    rm *monitor*.csv
    sed -i "s/mrv,/static,/g" *.csv
    tail -n+2 *.csv >> ../reproducibility/results/fig9/results.csv
    rm *.csv
}


fig10 () {
    echo "Running ${FUNCNAME[0]} $*"
    mkdir -p results/fig10
    cd ../microbench

    # database
    _setMicrobenchDb postgres

    # shared confs
    _set micro clients "[32]"
    _set micro sizes "[32]"
    _set micro initialStocks "[1000]"
    _set micro opDistribution "uneven"
    _set micro unevenScales "[1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100]"

    # normal, mrv, phaseReconciliation
    _set micro types "[normal, mrv, phaseReconciliation]"
    mvn exec:java -Dexec.mainClass="Main"
    rm *monitor*.csv
    mv *.csv ../reproducibility/results/fig10/results.csv

    # static
    _set micro types "[mrv]"
    _set micro workers "none"
    mvn exec:java -Dexec.mainClass="Main"
    rm *monitor*.csv
    sed -i "s/mrv,/static,/g" *.csv
    tail -n+2 *.csv >> ../reproducibility/results/fig10/results.csv
    rm *.csv
}


fig11 () {
    echo "Running ${FUNCNAME[0]} $*"
    mkdir -p results/fig11
    cd ../DBx1000
    rm -rf results

    # run
    _set dbx1000 TYPES "(NATIVE MRV ESCROW)"
    _set dbx1000 THREADS "(32)"
    _set dbx1000 WAREHOUSES "(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)"
    ./run.sh
    python3 output_to_csv.py results
    mv out.csv ../reproducibility/results/fig11/results.csv
    rm -r results
}


fig12 () {
    echo "Running ${FUNCNAME[0]} $*"
    mkdir -p results/fig12
    cd ../microbench

    # database
    _setMicrobenchDb postgres

    # microbenchmark
    if [[ -z $1 || $1 == "micro" ]]; then
        _set micro clients "[1, 2, 8, 32, 64, 128, 512]"
        _set micro sizes "[1, 2, 8, 32, 128, 512, 2048]"
        _set micro types "[normal, mrv]"
        mvn exec:java -Dexec.mainClass="Main"
        rm *monitor*.csv
        mv *.csv ../reproducibility/results/fig12/microbench.csv
    fi

    # tpc-c
    if [[ -z $1 || $1 == "tpcc" ]]; then
        cd ../tpcc
        _set tpcc THREADS "(1 2 8 32 64 128 512)"
        _set tpcc WAREHOUSES "(1 2 4 8 16 32 64)"
        ./run.sh
        python3 output_to_csv.py results
        mv out.csv ../reproducibility/results/fig12/tpcc.csv
        rm -rf results
        rm -f ../generic/mrvgenericworker/*.csv
    fi

    # stamp vacation
    if [[ -z $1 || $1 == "vacation" ]]; then
        cd ../stamp_vacation
        _set vacation CLIENTS "(1 2 8 32 64 128 512)"
        _set vacation RELATIONS "(1 2 8 32 128 512 2048)"
        ./run.sh
        python3 output_to_csv.py results
        mv out.csv ../reproducibility/results/fig12/vacation.csv
        rm -rf results
        rm -f ../generic/mrvgenericworker/*.csv
    fi
}


fig13 () {
    echo "Running ${FUNCNAME[0]} $*"
    if ! [[ $1 == +(postgres|mongodb|mysql) ]]; then
        echo "${FUNCNAME[0]}: Invalid arguments: must be 'postgres', 'mongodb', or 'mysql'"
        exit 1
    fi

    mkdir -p results/fig13
    cd ../microbench

    # database
    _setMicrobenchDb $1
    _setup_cluster $1 start

    # execute
    _set micro clients "[1, 2, 8, 32, 64, 128, 512]"
    _set micro sizes "[1, 2, 8, 32, 128, 512, 2048]"
    _set micro types "[normal, mrv]"
    if [[ $1 == "mysql" ]]; then
        _set micro maxNodes "2048"
    fi
    mvn exec:java -Dexec.mainClass="Main"
    rm *monitor*.csv
    mv *.csv ../reproducibility/results/fig13/$1.csv

    _setup_cluster $1 remove
}


fig14 () {
    echo "Running ${FUNCNAME[0]} $*"
    mkdir -p results/fig14
    cd ../DBx1000
    rm -rf results

    # shared confs
    _set dbx1000 THREADS "(32)"
    _set dbx1000 WAREHOUSES "(1 2 4 8 16 32)"
    _set dbx1000 CC "(WAIT_DIE MVCC HEKATON TICTOC SILO)"

    # without mrvs
    _set dbx1000 TYPES "(NATIVE)"
    ./run.sh
    python3 output_to_csv.py results
    sed -i "s/WAIT_DIE/2PL/" out.csv
    mv out.csv ../reproducibility/results/fig14/without_mrvs.csv
    rm -r results

    # with mrvs
    _set dbx1000 TYPES "(MRV)"
    ./run.sh
    python3 output_to_csv.py results
    sed -i "s/WAIT_DIE/2PL/" out.csv
    mv out.csv ../reproducibility/results/fig14/with_mrvs.csv
    rm -r results

    # escrow
    _set dbx1000 TYPES "(ESCROW)"
    _set dbx1000 CC "(WAIT_DIE)"
    ./run.sh
    python3 output_to_csv.py results
    sed -i "s/WAIT_DIE/ESCROW/" out.csv
    tail -n+2 out.csv >> ../reproducibility/results/fig14/without_mrvs.csv
    rm out.csv
    rm -r results
}


tab4() {
    echo "Running ${FUNCNAME[0]} $*"
    mkdir -p results/tab4
    cd ../microbench
    clients=(1 2 4 8 16 32 64 128 256 512)
    echo "type,clients,size" > ../reproducibility/results/tab4/results.csv
    _setMicrobenchDb postgres

    # 1 hotspot
    _set micro sizes "[1]"
    _set micro types "[mrv]"
    _set micro initialNodes "[1]"
    for c in "${clients[@]}"; do
        _set micro clients "[$c]"
        mvn exec:java -Dexec.mainClass="Main"
        size=$(PGPASSWORD=postgres psql -U postgres -h 127.0.0.1 -d testdb -A -t -c "select count(*) from product_stock")
        echo "1-hotspot,$c,$size" >> ../reproducibility/results/tab4/results.csv
        PGPASSWORD=postgres dropdb -U postgres -h 127.0.0.1 testdb
        PGPASSWORD=postgres createdb -U postgres -h 127.0.0.1 testdb
    done

    # 1 column mrv and native
    _set micro sizes "[100000]"
    _set micro accessDistribution "powerlaw"
    _set micro initialNodes "[1]"
    for c in "${clients[@]}"; do
        _set micro clients "[$c]"
        # native
        _set micro types "[normal]"
        mvn exec:java -Dexec.mainClass="Main"
        size=$(PGPASSWORD=postgres psql -U postgres -h 127.0.0.1 -d testdb -A -t -c "select pg_total_relation_size('product')")
        echo "1-column-native,$c,$size" >> ../reproducibility/results/tab4/results.csv
        PGPASSWORD=postgres dropdb -U postgres -h 127.0.0.1 testdb
        PGPASSWORD=postgres createdb -U postgres -h 127.0.0.1 testdb
        # mrv
        _set micro types "[mrv]"
        mvn exec:java -Dexec.mainClass="Main"
        size=$(PGPASSWORD=postgres psql -U postgres -h 127.0.0.1 -d testdb -A -t -c "select pg_total_relation_size('product_orig') + pg_total_relation_size('product_stock')")
        echo "1-column-mrv,$c,$size" >> ../reproducibility/results/tab4/results.csv
        PGPASSWORD=postgres dropdb -U postgres -h 127.0.0.1 testdb
        PGPASSWORD=postgres createdb -U postgres -h 127.0.0.1 testdb
    done
    # 1 column static (just one run since it takes a long time to populate; size should remain constant)
    _set micro clients "[1]"
    _set micro workers "none"
    _set micro initialNodes "[180]"
    mvn exec:java -Dexec.mainClass="Main"
    size=$(PGPASSWORD=postgres psql -U postgres -h 127.0.0.1 -d testdb -A -t -c "select pg_total_relation_size('product_orig') + pg_total_relation_size('product_stock')")
    echo "1-column-static,1,$size" >> ../reproducibility/results/tab4/results.csv
    PGPASSWORD=postgres dropdb -U postgres -h 127.0.0.1 testdb
    PGPASSWORD=postgres createdb -U postgres -h 127.0.0.1 testdb

    # tpc-c
    cd ../tpcc
    _set tpcc WAREHOUSES "(1)"
    for c in "${clients[@]}"; do
        _set tpcc THREADS "($c)"
        # native
        _set tpcc TYPES "(native)"
        ./run.sh
        size=$(PGPASSWORD=postgres psql -U postgres -h 127.0.0.1 -d testdb -A -t -c "select pg_database_size('testdb')")
        echo "tpcc-native,$c,$size" >> ../reproducibility/results/tab4/results.csv
        PGPASSWORD=postgres dropdb -U postgres -h 127.0.0.1 testdb
        PGPASSWORD=postgres createdb -U postgres -h 127.0.0.1 testdb
        # mrv
        _set tpcc TYPES "(mrv)"
        ./run.sh
        size=$(PGPASSWORD=postgres psql -U postgres -h 127.0.0.1 -d testdb -A -t -c "select pg_database_size('testdb')")
        echo "tpcc-mrv,$c,$size" >> ../reproducibility/results/tab4/results.csv
        PGPASSWORD=postgres dropdb -U postgres -h 127.0.0.1 testdb
        PGPASSWORD=postgres createdb -U postgres -h 127.0.0.1 testdb
    done
}


_finish () {
    popd > /dev/null
    _recover_confs
    _clean
}

list_of_experiments="List of experiments:
  fig4 (postgres|mongodb) [read|write]
  fig 5
  fig 6
  fig 7
  fig 8 [balance|adjust]
  fig 9
  fig 10
  fig 11
  fig 12 [micro|tpcc|vacation]
  fig 13 (postgres|mongodb|mysql)
  fig 14
  tab4"

_recover_confs
_init
# comment server list in the microbenchmark (this will later be added inline)
sed -i "s|- 127.0.0.1:5432|#- 127.0.0.1:5432|" ../microbench/src/main/resources/config.yaml
mkdir -p results
pushd . > /dev/null
trap _finish EXIT

# no arguments - run all tests
if [ $# -eq 0 ]; then
    echo "Running all experiments"
    ./run_experiments.sh fig4 postgres
    ./run_experiments.sh fig4 mongodb
    ./run_experiments.sh fig5
    ./run_experiments.sh fig6
    ./run_experiments.sh fig7
    ./run_experiments.sh fig8
    ./run_experiments.sh fig9
    ./run_experiments.sh fig10
    ./run_experiments.sh fig11
    ./run_experiments.sh fig12
    ./run_experiments.sh fig13 postgres
    ./run_experiments.sh fig13 mongodb
    ./run_experiments.sh fig13 mysql
    ./run_experiments.sh fig14
    ./run_experiments.sh tab4
# experiment does not exist
elif !(compgen -A function | grep -q "$1") ; then
    echo "Experiment not found: $1"
    echo "$list_of_experiments"
    exit 1
# run single experiment
else
    $1 ${@:2}
fi
