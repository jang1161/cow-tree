#!/bin/bash

set -o pipefail

DEVICE="${DEVICE:-/dev/nvme3n2}"
TESTFILE="${BINARY:-./build/bin/cow-bench-ram_async}"
OUTFILE="${OUTFILE:-result_tmp.txt}"

KEYS=(10000 100000 1000000 10000000)
THREADS=(1 2 4 8 16 32 64) # 64

if [[ ! -x "$TESTFILE" ]]; then
    echo "[INFO] benchmark binary not found: $TESTFILE" >&2
    echo "[INFO] Build first: make bench-ram_async" >&2
    exit 1
fi

rm -f "$OUTFILE"
touch "$OUTFILE"

run_test() {

    KEYNUM=$1
    LABEL=$2
    RUN_COUNT=$3 

    echo "$LABEL" | tee -a "$OUTFILE"

    printf "%-10s" "threads" | tee -a "$OUTFILE"
    for t in "${THREADS[@]}"; do
        printf "%10s" "$t" | tee -a "$OUTFILE"
    done
    echo "" | tee -a "$OUTFILE"

    declare -a sums
    for ((i=0;i<7;i++)); do
        sums[$i]=0
    done

    for ((run=1; run<=RUN_COUNT; run++))
    do
        printf "%-10s" "Run#$run" | tee -a "$OUTFILE"

        for i in ${!THREADS[@]}
        do
            t=${THREADS[$i]}

            echo ""
            echo "Running: keys=$KEYNUM threads=$t run=$run"

            out=$(sudo $TESTFILE $KEYNUM $t $DEVICE 2>&1 | tee /dev/tty)
            cmd_status=$?

            thr=$(echo "$out" | awk '/Average throughput/ {v=$3} END {print v}')

            if [[ $cmd_status -ne 0 || -z "$thr" || ! "$thr" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
                echo "[WARN] throughput parse failed: keys=$KEYNUM threads=$t run=$run status=$cmd_status thr='$thr'" >&2
                printf "%10s" "ERR" | tee -a "$OUTFILE"
                continue
            fi

            printf "%10.0f" "$thr" | tee -a "$OUTFILE"

            sums[$i]=$(echo "${sums[$i]} + $thr" | bc)
        done

        echo "" | tee -a "$OUTFILE"
    done

    printf "%-10s" "Average" | tee -a "$OUTFILE"

    for i in ${!THREADS[@]}
    do
        avg=$(echo "scale=2; ${sums[$i]} / $RUN_COUNT" | bc)
        printf "%10.0f" "$avg" | tee -a "$OUTFILE"
    done

    echo "" | tee -a "$OUTFILE"
    echo "" | tee -a "$OUTFILE"
}

run_test 100000 "100K" 3
run_test 1000000 "1M" 1
# run_test 10000000 "10M" 1

echo "Benchmark finished. Results saved to $OUTFILE"