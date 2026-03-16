#!/bin/bash

DEVICE="/dev/nvme3n2"
OUTFILE="result.txt"

KEYS=(10000 50000 100000 1000000)
THREADS=(1 2 4 8 16 32 64)

# rm -f $OUTFILE
# touch $OUTFILE

run_test() {

    KEYNUM=$1
    LABEL=$2

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

    for run in 1 2 3
    do
        printf "%-10s" "Run#$run" | tee -a $OUTFILE

        for i in ${!THREADS[@]}
        do
            t=${THREADS[$i]}

            echo ""
            echo "Running: keys=$KEYNUM threads=$t run=$run"

            # 터미널 출력 + 변수 저장
            out=$(sudo ./cow_test_v3 $KEYNUM $t $DEVICE | tee /dev/tty)

            thr=$(echo "$out" | grep "Average throughput" | awk '{print $3}')

            printf "%10.0f" "$thr" | tee -a $OUTFILE

            sums[$i]=$(echo "${sums[$i]} + $thr" | bc)
        done

        echo "" | tee -a $OUTFILE
    done

    printf "%-10s" "Average" | tee -a $OUTFILE

    for i in ${!THREADS[@]}
    do
        avg=$(echo "scale=2; ${sums[$i]} / 3" | bc)
        printf "%10.0f" "$avg" | tee -a $OUTFILE
    done

    echo "" | tee -a $OUTFILE
    echo "" | tee -a $OUTFILE
}

# run_test 10000 "10K"
# run_test 50000 "50K"
# run_test 100000 "100K"
run_test 1000000 "1M"

echo "Benchmark finished. Results saved to $OUTFILE"