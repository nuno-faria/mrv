sudo docker-compose --compatibility down
sudo docker volume rm $(sudo docker volume ls -qf dangling=true)