sudo docker-compose --compatibility up -d

until sudo docker-compose --compatibility exec node1 mysql -uroot -proot -e "select 1" &> /dev/null; do
  echo "Waiting for all nodes to start ..."
  sleep 5
done

sudo docker-compose --compatibility exec node1 mysql -uroot -proot \
  -e "SET @@GLOBAL.group_replication_bootstrap_group=1;" \
  -e "create user 'repl'@'%';" \
  -e "GRANT REPLICATION SLAVE ON *.* TO repl@'%';" \
  -e "flush privileges;" \
  -e "change master to master_user='root' for channel 'group_replication_recovery';" \
  -e "START GROUP_REPLICATION;" \
  -e "SET @@GLOBAL.group_replication_bootstrap_group=0;" \
  -e "SELECT * FROM performance_schema.replication_group_members;"
sleep 3

for N in 2 3
do sudo docker-compose --compatibility exec node$N mysql -uroot -proot \
  -e "change master to master_user='repl' for channel 'group_replication_recovery';" \
  -e "START GROUP_REPLICATION;"
done

for N in 1 2 3
do sudo docker-compose --compatibility exec node$N mysql -uroot -proot \
  -e "SHOW VARIABLES WHERE Variable_name = 'hostname';" \
  -e "SELECT * FROM performance_schema.replication_group_members;"
done

sudo docker-compose --compatibility exec node1 mysql -uroot -proot \
  -e "CREATE DATABASE testdb;" \
  -e "CREATE USER 'root'@'%' IDENTIFIED BY 'root';" \
  -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;"