#!/bin/bash

# setup
sudo chmod +x setup.sh
./setup.sh

# run
./run_experiments.sh

# plot
./plot_experiments.sh

# document
cd document
make
cd ..
