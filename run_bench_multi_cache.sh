#!/bin/bash

DEVICE="/dev/nvme3n2"
BINARY="./cow_test_v3_multi-cache"
OUTFILE="result_multi_cache_10M.txt"
RUNS=1

KEYS=(10000 50000 100000 1000000)
THREADS=(1 2 4 8 16 32 64)

# 숫자 파싱 헬퍼: 빈 값이면 0 반환
num() { echo "${1:-0}"; }

rm -f "$OUTFILE"
touch "$OUTFILE"

tee_out() { tee -a "$OUTFILE"; }

run_test() {
    local keynum=$1
    local label=$2

    echo "$label" | tee_out
    printf "%-10s %12s %12s %12s %12s %12s %12s %12s %12s %12s\n" \
        "threads" "throughput" "cache_hit" "cache_miss" "hit_ratio%" \
        "evictions" "odirect_rd" "qlock_ins" "qlock_w" "batch_sz" | tee_out

    for t in "${THREADS[@]}"; do
        sum_thr=0
        sum_hit=0
        sum_miss=0
        sum_ratio=0
        sum_evict=0
        sum_odirect=0
        sum_qlock_ins=0
        sum_qlock_w=0
        sum_batch=0

        for run in $(seq 1 $RUNS); do
            echo "  [keys=$keynum threads=$t run=$run]" >&2
            output=$(sudo $BINARY $keynum $t $DEVICE 2>&1)

            thr=$(echo "$output"   | grep "Average throughput"          | awk '{print $3}')
            hit=$(echo "$output"   | grep "\[multiway-cache\] global_cache hit=" \
                                   | grep -oP '(?<=hit=)\d+')
            miss=$(echo "$output"  | grep "\[multiway-cache\] global_cache" \
                                   | grep -oP '(?<=miss=)\d+')
            ratio=$(echo "$output" | grep "\[multiway-cache\] global_cache" \
                                   | grep -oP '(?<=hit_ratio=)[0-9.]+')
            evict=$(echo "$output" | grep "\[multiway-cache\] set_conflict_evictions=" \
                                   | grep -oP '(?<=set_conflict_evictions=)\d+')
            odirect=$(echo "$output" | grep "\[multiway-cache\] odirect_reads=" \
                                     | grep -oP '(?<=odirect_reads=)\d+')
            qins=$(echo "$output"  | grep "\[multiway-cache\] q_lock_wait_avg_us" \
                                   | grep -oP '(?<=insert=)[0-9.]+')
            qw=$(echo "$output"    | grep "\[multiway-cache\] q_lock_wait_avg_us" \
                                   | grep -oP '(?<=writer=)[0-9.]+')
            batch=$(echo "$output" | grep "\[multiway-cache\] avg_batch_size=" \
                                   | grep -oP '(?<=avg_batch_size=)[0-9.]+')

            sum_thr=$(echo      "$sum_thr   + $(num $thr)"     | bc)
            sum_hit=$(echo      "$sum_hit   + $(num $hit)"     | bc)
            sum_miss=$(echo     "$sum_miss  + $(num $miss)"    | bc)
            sum_ratio=$(echo    "$sum_ratio + $(num $ratio)"   | bc)
            sum_evict=$(echo    "$sum_evict + $(num $evict)"   | bc)
            sum_odirect=$(echo  "$sum_odirect + $(num $odirect)" | bc)
            sum_qlock_ins=$(echo "$sum_qlock_ins + $(num $qins)" | bc)
            sum_qlock_w=$(echo  "$sum_qlock_w + $(num $qw)"   | bc)
            sum_batch=$(echo    "$sum_batch + $(num $batch)"   | bc)
        done

        avg_thr=$(echo     "scale=2; $sum_thr     / $RUNS" | bc)
        avg_hit=$(echo     "scale=0; $sum_hit     / $RUNS" | bc)
        avg_miss=$(echo    "scale=0; $sum_miss    / $RUNS" | bc)
        avg_ratio=$(echo   "scale=2; $sum_ratio   / $RUNS" | bc)
        avg_evict=$(echo   "scale=0; $sum_evict   / $RUNS" | bc)
        avg_odirect=$(echo "scale=0; $sum_odirect / $RUNS" | bc)
        avg_qins=$(echo    "scale=2; $sum_qlock_ins / $RUNS" | bc)
        avg_qw=$(echo      "scale=2; $sum_qlock_w / $RUNS" | bc)
        avg_batch=$(echo   "scale=2; $sum_batch   / $RUNS" | bc)

        printf "%-10d %12s %12s %12s %12s %12s %12s %12s %12s %12s\n" \
            "$t" "$avg_thr" "$avg_hit" "$avg_miss" "$avg_ratio" \
            "$avg_evict" "$avg_odirect" "$avg_qins" "$avg_qw" "$avg_batch" | tee_out
    done

    echo "" | tee_out
}

# run_test  10000 "10K"
# run_test  50000 "50K"
# run_test 100000 "100K"
# run_test 1000000 "1M"
run_test 10000000 "10M"

echo "Done. Results saved to $OUTFILE"
