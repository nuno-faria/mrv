## Reproducibility

This folder contains the scripts to setup the environment, run all tests, and plot the figures contained in the paper. The only prior requirement is the actual instance to run the tests.

Running all tests can be done with a single command and takes approximately 29 hours. Setting up the system can be done with a single script and takes approximately 5 minutes.


### Instance to match the paper environment

- 24 vCPU cores
- 24 GB RAM
- 500 GB SSD
- Ubuntu 18.04 | 20.04 | 22.04 (x86_64)
- Internet connection
- Must support virtualization (to install Docker)

(The original tests ran on Google Cloud Engine virtual machines, using N1 vCPUs and persistent SSDs.)

**Note: to better simulate the paper results, ensure that an SSD is used for the databases' storage.**


### Setup

Installs the databases and benchmarks.

```shell
sudo chmod +x setup.sh
./setup.sh
```


### Run experiments

The `run_experiments.sh` script runs every experiment in the paper. Optionally, to run a specific experiment, we can provide its name, according to the label in the paper.

```shell
./run_experiments.sh [experiment_name [args]]
```

List of experiments:
```txt
fig4 (postgres|mongodb) [read|write]
fig5
fig6
fig7
fig8 [balance|adjust]
fig9
fig10
fig11
fig12 [micro|tpcc|vacation]
fig13 (postgres|mongodb|mysql)
fig14
tab4
```


### Plot results

The `plot_experiments.sh` script plots the experiments in the same way as the paper. By default, all results in the `results` folder are plotted, however, specific results can be plotted by providing their name as argument.

```shell
./plot_experiments.sh [e1, e2, ..., en]
```


### PDF Document

There is also a LaTeX file to easily visualize the results, combining all plots with their original labels and captions.

```shell
cd document
make
# outputs a document.pdf file
```


### Single script

The `run_everything.sh` script can be used to setup the system, run the experiments, plot the results, and build the final document.

```shell
sudo chmod +x run_everything.sh
./run_everything.sh
```


### Main differences from the paper

- For simplicity, MongoDB and MySQL Group Replication run in Docker containers instead of natively in standalone machines;
- The *System X* results are not available, as this is a cloud-native closed-sourced database system which we are not allowed to disclose;
- Some minor optimizations/fixes were made in the MRV code. The paper's conclusions are, however, still the same.
