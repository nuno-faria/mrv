#!/bin/bash

if [ "$#" -lt 3 ]; then
    echo "Usage: ./start_mysqlgr.sh <server-id> <server-ip> <other-server-ip other-server-ip ...>"
    exit 1
fi

seeds=--loose-group-replication-group-seeds=$2:6606

for i in "${@:3}"
do
    seeds="${seeds},${i}:6606"
done

sudo docker run --name=mysqlgr --network=host -e MYSQL_ROOT_PASSWORD=root -d mysql/mysql-server:8.0.17 mysqld --server-id=$1 --log-bin=mysql-bin-1.log --enforce-gtid-consistency=ON --log-slave-updates=ON --gtid-mode=ON --transaction-write-set-extraction=XXHASH64 --binlog-checksum=NONE --master-info-repository=TABLE --relay-log-info-repository=TABLE --plugin-load=group_replication.so --relay-log-recovery=ON --loose-slave-parallel-workers=1024 --loose-slave-preserve-commit-order=1 --loose-slave-parallel-type=LOGICAL_CLOCK --loose-group-replication-start-on-boot=OFF --loose-group-replication-group-name=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee --loose-group-replication-local-address=$2:6606 $seeds --loose-group-replication-single-primary-mode=OFF --loose-group-replication-enforce-update-everywhere-checks=ON --group_replication_ip_whitelist=0.0.0.0/0 --max_connections=5000
