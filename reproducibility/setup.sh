#!/bin/bash

cd ..
sudo find ./deploy -type f -iname "*.sh" -exec chmod +x {} \;
./deploy/install_docker.sh
./deploy/install_java.sh
./deploy/install_python.sh
./deploy/install_sysbench.sh
./deploy/postgres/install_postgres.sh
sudo DEBIAN_FRONTEND=noninteractive apt install texlive-latex-base texlive-latex-extra texlive-fonts-extra -y &
tex_pid=$!
cd deploy/mysqlgr
./deploy_cluster.sh
./remove_cluster.sh
cd ../mongodb
./deploy_cluster.sh
./remove_cluster.sh
sudo bash -c "echo '127.0.0.1 mongo1' >> /etc/hosts";
sudo bash -c "echo '127.0.0.1 mongo2' >> /etc/hosts";
sudo bash -c "echo '127.0.0.1 mongo3' >> /etc/hosts";
cd ../../microbench
mvn clean install
cd ../tpcc
sudo chmod +x *.sh *.lua
cd ../stamp_vacation
sudo chmod +x run.sh
cd ../generic/mrvgenericworker
mvn clean install
cd ../../DBx1000
sudo chmod +x *.sh
cd ../reproducibility/plot
pip3 install -r requirements.txt
cd ..
sudo chmod +x *.sh
wait $tex_pid
