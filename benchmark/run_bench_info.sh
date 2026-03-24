#!/bin/bash

DEVICE="${DEVICE:-/dev/nvme3n2}"
BINARY="${BINARY:-./build/bin/cow-bench-ram}"
OUTFILE="${OUTFILE:-result_ram_profile_10M.txt}"

THREADS=(1 2 4 8 16 32 64)

# 숫자 파싱 헬퍼: 빈 값이면 0 반환
num() { echo "${1:-0}"; }

if [[ ! -x "$BINARY" ]]; then
    echo "[INFO] benchmark binary not found: $BINARY" >&2
    echo "[INFO] Build first: make bench-ram" >&2
    exit 1
fi

rm -f "$OUTFILE"
touch "$OUTFILE"

tee_out() { tee -a "$OUTFILE"; }

run_test() {
    local keynum=$1
    local label=$2
    local runs=$3

    echo "$label" | tee_out
    printf "%-8s %10s %9s %9s %9s %10s %10s %10s %10s %9s %9s %9s\n" \
        "threads" "throughput" "qins_us" "qw_us" "batch" "qdepth" "ov_nodes" "ap_retry" "zone_rot" "app_us" "bt_us" "appends" | tee_out

    for t in "${THREADS[@]}"; do
        sum_thr=0
        sum_qlock_ins=0
        sum_qlock_w=0
        sum_batch=0
        sum_qdepth=0
        sum_overlay=0
        sum_append_retry=0
        sum_zone_rot=0
        sum_append_lat=0
        sum_batch_lat=0
        sum_page_appends=0

        for run in $(seq 1 $runs); do
            echo "  [keys=$keynum threads=$t run=$run]" >&2
            output=$(sudo $BINARY $keynum $t $DEVICE 2>&1)

            thr=$(echo "$output"   | grep "Average throughput"          | awk '{print $3}')

            qline=$(echo "$output" | grep "\[ram\] q_lock_wait_avg_us" | tail -n1)
            bline=$(echo "$output" | grep "\[ram\] avg_batch_size=" | tail -n1)
            qdline=$(echo "$output" | grep "\[ram\] queue_depth" | tail -n1)
            ovline=$(echo "$output" | grep "\[ram\] overlay_nodes" | tail -n1)
            arline=$(echo "$output" | grep "\[ram\] append_retries=" | tail -n1)
            lline=$(echo "$output" | grep "\[ram\] sampled_latency_us" | tail -n1)
            pline=$(echo "$output" | grep "\[ram\] page_appends=" | tail -n1)

            qins=$(echo "$qline" | sed -n 's/.*insert=\([0-9.]*\).*/\1/p')
            qw=$(echo "$qline" | sed -n 's/.*writer=\([0-9.]*\).*/\1/p')
            batch=$(echo "$bline" | sed -n 's/.*avg_batch_size=\([0-9.]*\).*/\1/p')
            qdepth=$(echo "$qdline" | sed -n 's/.*avg=\([0-9.]*\).*/\1/p')
            overlay=$(echo "$ovline" | sed -n 's/.*avg=\([0-9.]*\).*/\1/p')
            append_retry=$(echo "$arline" | sed -n 's/.*append_retries=\([0-9.]*\).*/\1/p')
            zone_rot=$(echo "$arline" | sed -n 's/.*zone_rotations=\([0-9.]*\).*/\1/p')
            append_lat=$(echo "$lline" | sed -n 's/.*append=\([0-9.]*\).*/\1/p')
            batch_lat=$(echo "$lline" | sed -n 's/.*batch=\([0-9.]*\).*/\1/p')
            page_appends=$(echo "$pline" | sed -n 's/.*page_appends=\([0-9.]*\).*/\1/p')

            sum_thr=$(echo      "$sum_thr   + $(num $thr)"     | bc)
            sum_qlock_ins=$(echo "$sum_qlock_ins + $(num $qins)" | bc)
            sum_qlock_w=$(echo  "$sum_qlock_w + $(num $qw)"   | bc)
            sum_batch=$(echo    "$sum_batch + $(num $batch)"   | bc)
            sum_qdepth=$(echo   "$sum_qdepth + $(num $qdepth)" | bc)
            sum_overlay=$(echo  "$sum_overlay + $(num $overlay)" | bc)
            sum_append_retry=$(echo "$sum_append_retry + $(num $append_retry)" | bc)
            sum_zone_rot=$(echo "$sum_zone_rot + $(num $zone_rot)" | bc)
            sum_append_lat=$(echo "$sum_append_lat + $(num $append_lat)" | bc)
            sum_batch_lat=$(echo "$sum_batch_lat + $(num $batch_lat)" | bc)
            sum_page_appends=$(echo "$sum_page_appends + $(num $page_appends)" | bc)
        done

        avg_thr=$(echo        "scale=2; $sum_thr / $runs" | bc)
        avg_qins=$(echo       "scale=2; $sum_qlock_ins / $runs" | bc)
        avg_qw=$(echo         "scale=2; $sum_qlock_w / $runs" | bc)
        avg_batch=$(echo      "scale=2; $sum_batch / $runs" | bc)
        avg_qdepth=$(echo     "scale=2; $sum_qdepth / $runs" | bc)
        avg_overlay=$(echo    "scale=2; $sum_overlay / $runs" | bc)
        avg_ap_retry=$(echo   "scale=0; $sum_append_retry / $runs" | bc)
        avg_zone_rot=$(echo   "scale=0; $sum_zone_rot / $runs" | bc)
        avg_append_lat=$(echo "scale=2; $sum_append_lat / $runs" | bc)
        avg_batch_lat=$(echo  "scale=2; $sum_batch_lat / $runs" | bc)
        avg_appends=$(echo    "scale=0; $sum_page_appends / $runs" | bc)

        printf "%-8d %10s %9s %9s %9s %10s %10s %10s %10s %9s %9s %9s\n" \
            "$t" "$avg_thr" "$avg_qins" "$avg_qw" "$avg_batch" "$avg_qdepth" \
            "$avg_overlay" "$avg_ap_retry" "$avg_zone_rot" "$avg_append_lat" \
            "$avg_batch_lat" "$avg_appends" | tee_out
    done

    echo "" | tee_out
}

# run_test 100000 "100K" 3
# run_test 1000000 "1M" 3
run_test 10000000 "10M" 3

echo "Done. Results saved to $OUTFILE"
