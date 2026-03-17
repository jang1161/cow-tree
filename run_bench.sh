#!/bin/bash

DEVICE="/dev/nvme3n2"
TESTFILE="./cow_test_ram"
OUTFILE="result_ram.txt"

KEYS=(10000 50000 100000 1000000)
THREADS=(1 2 4 8 16 32 64)

rm -f $OUTFILE
touch $OUTFILE

run_test() {

    KEYNUM=$1
    LABEL=$2
    RUN_COUNT=$3 

    echo "$LABEL" | tee -a $OUTFILE

    printf "%-10s" "threads" | tee -a $OUTFILE
    for t in "${THREADS[@]}"; do
        printf "%10s" "$t" | tee -a $OUTFILE
    done
    echo "" | tee -a $OUTFILE

    declare -a sums
    for ((i=0;i<7;i++)); do
        sums[$i]=0
    done

    for ((run=1; run<=RUN_COUNT; run++))
    do
        printf "%-10s" "Run#$run" | tee -a $OUTFILE

        for i in ${!THREADS[@]}
        do
            t=${THREADS[$i]}

            echo ""
            echo "Running: keys=$KEYNUM threads=$t run=$run"

            out=$(sudo $TESTFILE $KEYNUM $t $DEVICE | tee /dev/tty)

            thr=$(echo "$out" | grep "Average throughput" | awk '{print $3}')

            printf "%10.0f" "$thr" | tee -a $OUTFILE

            sums[$i]=$(echo "${sums[$i]} + $thr" | bc)
        done

        echo "" | tee -a $OUTFILE
    done

    printf "%-10s" "Average" | tee -a $OUTFILE

    for i in ${!THREADS[@]}
    do
        avg=$(echo "scale=2; ${sums[$i]} / $RUN_COUNT" | bc)  # 🔥 수정
        printf "%10.0f" "$avg" | tee -a $OUTFILE
    done

    echo "" | tee -a $OUTFILE
    echo "" | tee -a $OUTFILE
}

run_test 10000 "10K" 3
run_test 50000 "50K" 3
run_test 100000 "100K" 3
run_test 1000000 "1M" 3

# 10M은 1번만 실행
run_test 10000000 "10M" 1

echo "Benchmark finished. Results saved to $OUTFILE"