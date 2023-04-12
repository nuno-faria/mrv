#!/bin/bash

# ./populate.sh <host> <port> <username> <password> <warehouses>

PGPASSWORD=$4 dropdb -h $1 -p $2 -U $3 testdb
PGPASSWORD=$4 createdb -h $1 -p $2 -U $3 testdb
PGPASSWORD=$4 psql -h $1 -p $2 -U $3 -d testdb -c "create table tx_status (table_name varchar, column_name varchar, pk varchar, commits int, aborts int, last_updated timestamp, pk_sql varchar, primary key(table_name, column_name, pk))"
./tpcc.lua \
    --pgsql-host=$1 \
    --pgsql-port=$2 \
    --pgsql-user=$3 \
    --pgsql-password=$4 \
    --pgsql-db=testdb \
    --threads=4 \
    --report-interval=1 \
    --tables=1 \
    --scale=$5 \
    --use_fk=0  \
    --trx_level=RR \
    --db-driver=pgsql \
    prepare
PGPASSWORD=$4 psql -h $1 -p $2 -U $3 -d testdb -c "vacuum analyze"
