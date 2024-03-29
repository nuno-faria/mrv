#!/bin/bash

# tests different concurency controls with and without mrvs
# NOTE: ESCROW was only implemented with WAIT_DIE


# vs escrow parameters

CC=(WAIT_DIE)
ISOLATIONS=(SERIALIZABLE)
TYPES=(NATIVE MRV ESCROW)
MRV_SIZES=(128)
MAX_TXN_PER_PART=(100000)
THREADS=(32)
WAREHOUSES=(1 2 4 8 16 32)
MODES=(TPC_C)


# various ccs parameters

#CC=(WAIT_DIE MVCC HEKATON TICTOC SILO)
#ISOLATIONS=(SERIALIZABLE)
#TYPES=(NATIVE MRV)
#MRV_SIZES=(128)
#MAX_TXN_PER_PART=(100000)
#THREADS=(32)
#WAREHOUSES=(1 2 4 8 16 32)
#MODES=(TPC_C)

mkdir -p results

for mode in "${MODES[@]}"; do
    for type in "${TYPES[@]}"; do
        for cc in "${CC[@]}"; do
            for isolation in "${ISOLATIONS[@]}"; do
                for max_tx in "${MAX_TXN_PER_PART[@]}"; do
                    for threads in "${THREADS[@]}"; do
                        for warehouses in "${WAREHOUSES[@]}"; do
                            sed -i "s/#define CC_ALG .*/#define CC_ALG ${cc}/" config.h
                            sed -i "s/#define ISOLATION_LEVEL .*/#define ISOLATION_LEVEL ${isolation}/" config.h
                            sed -i "s/#define TYPE .*/#define TYPE ${type}/" config.h
                            sed -i "s/#define MAX_TXN_PER_PART .*/#define MAX_TXN_PER_PART ${max_tx}/" config.h
                            sed -i "s/#define MODE .*/#define MODE ${mode}/" config.h

                            if [[ "$type" == "MRV" ]]; then
                                for mrv_size in "${MRV_SIZES[@]}"; do
                                    sed -i "s/#define MRV_SIZE .*/#define MRV_SIZE ${mrv_size}/" config.h
                                    make -j >> /dev/null
                                    echo "running ${mode}-MRV-${cc}-${isolation}-T${threads}-W${warehouses}"
                                    ./rundb -t${threads} -n${warehouses} -o results/${mode}-MRV-${cc}-${isolation}-T${threads}-W${warehouses} >> /dev/null
                                    sleep 30 # cooldown
                                done
                            else
                                make -j >> /dev/null
                                echo "running ${mode}-${type}-${cc}-${isolation}-T${threads}-W${warehouses}"
                                ./rundb -t${threads} -n${warehouses} -o results/${mode}-${type}-${cc}-${isolation}-T${threads}-W${warehouses} >> /dev/null
                                sleep 30 # cooldown
                            fi
                        done
                    done
                done
            done
        done
    done
done