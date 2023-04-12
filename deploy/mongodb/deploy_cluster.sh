sudo docker-compose --compatibility up -d
echo "Waiting for all nodes to start ..."
sleep 45
sudo docker exec -it mongo1 mongo --port 27011 --eval "rs.initiate({_id: 'replica_set', members: [{ _id: 0, host: \"mongo1:27011\"}, { _id: 1, host: \"mongo2:27012\" },   { _id : 2, host : \"mongo3:27013\" } ] } )"
sleep 10
