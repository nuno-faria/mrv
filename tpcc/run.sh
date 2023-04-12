#!/bin/bash

# run commands are at the bottom of the file

HOST=localhost
PORT=5432
USER=postgres
PASSWORD=postgres
THREADS=(1 2 8 32 64 128 512)
WAREHOUSES=(1 2 4 8 16 32 64)
TIME=60
MRV_WORKERS=true # true | false
ISOLATION=RR # RR | RC

run () {
    if [ "$#" -eq 9 ]; then
        sed -i 's/add_status_dummy(/add_status(/g' tpcc_run.lua
        sed -i 's/add_status_dummy(/add_status(/g' tpcc.lua
    else
        sed -i 's/add_status(/add_status_dummy(/g' tpcc_run.lua
        sed -i 's/add_status(/add_status_dummy(/g' tpcc.lua
    fi

    ./tpcc.lua \
        --pgsql-host=$1 \
        --pgsql-port=$2 \
        --pgsql-user=$3 \
        --pgsql-password=$4 \
        --pgsql-db=testdb \
        --threads=$5 \
        --time=$6 \
        --report-interval=1 \
        --tables=1 \
        --scale=$7 \
        --use_fk=0  \
        --trx_level=$8 \
        --db-driver=pgsql \
        run
}


run_native () {
    for n_warehouses in "${WAREHOUSES[@]}"; do
        for n_threads in "${THREADS[@]}"; do
            echo "$n_threads THREADS"
            ./populate.sh $HOST $PORT $USER $PASSWORD $n_warehouses
            run $HOST $PORT $USER $PASSWORD $n_threads $TIME $n_warehouses $ISOLATION > results/normal_${n_warehouses}_${n_threads}.txt
            sleep 30
        done
    done
}


run_mrv () {
    cd ../generic/mrvgenericworker
    mvn clean install -q
    cd ../../tpcc

    sed -i "s|//.*:|//${HOST}:|" workers_config.yml
    sed -i -r "s|:[0-9]+/|:${PORT}/|" workers_config.yml
    sed -i "s|user=.*&|user=${USER}\&|" workers_config.yml
    sed -i "s|password=.*|password=${PASSWORD}|" workers_config.yml
    cp workers_config.yml ../generic/mrvgenericworker/src/main/resources/config.yml

    sed -i "s|host:.*|host: ${HOST}|" model_tpcc.yml
    sed -i "s|port:.*|port: ${PORT}|" model_tpcc.yml
    sed -i "s|user:.*|user: ${USER}|" model_tpcc.yml
    sed -i "s|password:.*|password: ${PASSWORD}|" model_tpcc.yml

    for n_warehouses in "${WAREHOUSES[@]}"; do
        for n_threads in "${THREADS[@]}"; do
            echo "$n_threads THREADS"
            ./populate.sh $HOST $PORT $USER $PASSWORD $n_warehouses
            cd ../generic
            python3 convert_model.py ../tpcc/model_tpcc.yml $n_threads
            if [ $MRV_WORKERS = "true" ]; then
                cd mrvgenericworker
                mvn exec:java -Dexec.mainClass="Main" &
                workers=$!
                sleep 5
                cd ..
            fi
            cd ../tpcc
            run $HOST $PORT $USER $PASSWORD $n_threads $TIME $n_warehouses $ISOLATION mrv > results/mrv_${n_warehouses}_${n_threads}.txt
            if [ $MRV_WORKERS = "true" ]; then
                kill $workers
            fi
            sleep 30
        done
    done
}


mkdir -p results
run_native
run_mrv
