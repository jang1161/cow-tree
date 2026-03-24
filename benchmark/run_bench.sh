#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DEFAULT_DEV="/dev/nvme3n2"
DEFAULT_OUTFILE="results/bench_$(date +%m%d%H%M).txt"
THREADS=(1 2 4 8 16 32 64)

REPEAT_100K=3
REPEAT_1M=3
REPEAT_10M=1
DEV="${DEV:-${DEVICE:-$DEFAULT_DEV}}"
OUTFILE="${OUTFILE:-$DEFAULT_OUTFILE}"

usage() {
    cat <<EOF
Usage:
  ./run_bench.sh <variant> 100K=<n> 1M=<n> 10M=<n> [DEV=<path>] [OUTFILE=<path>]
  ./run_bench.sh <variant> <repeat_100K> <repeat_1M> <repeat_10M> [device] [outfile]

Examples:
  ./run_bench.sh ram 100K=3 1M=3 10M=1 DEV=/dev/nvme3n2
  ./run_bench.sh ram 3 3 1 /dev/nvme3n2
  ./run_bench.sh ram 3 3 1 /dev/nvme3n2 results/custom.txt
EOF
}

is_uint() {
    [[ "$1" =~ ^[0-9]+$ ]]
}

if [[ $# -lt 1 ]]; then
    usage
    exit 1
fi

VARIANT="$1"
shift

NUMERIC_SEEN=0

while [[ $# -gt 0 ]]; do
    arg="$1"
    case "$arg" in
        100K=*|100k=*)
            REPEAT_100K="${arg#*=}"
            ;;
        1M=*|1m=*)
            REPEAT_1M="${arg#*=}"
            ;;
        10M=*|10m=*)
            REPEAT_10M="${arg#*=}"
            ;;
        DEV=*|dev=*|DEVICE=*|device=*)
            DEV="${arg#*=}"
            ;;
        OUTFILE=*|outfile=*)
            OUTFILE="${arg#*=}"
            ;;
        *)
            if is_uint "$arg"; then
                NUMERIC_SEEN=$((NUMERIC_SEEN + 1))
                case "$NUMERIC_SEEN" in
                    1) REPEAT_100K="$arg" ;;
                    2) REPEAT_1M="$arg" ;;
                    3) REPEAT_10M="$arg" ;;
                    *)
                        echo "[ERROR] too many numeric repeat args: $arg" >&2
                        usage
                        exit 1
                        ;;
                esac
            elif [[ "$arg" == */* && "$DEV" == "$DEFAULT_DEV" ]]; then
                DEV="$arg"
            elif [[ "$OUTFILE" == "$DEFAULT_OUTFILE" ]]; then
                OUTFILE="$arg"
            else
                echo "[ERROR] unknown argument: $arg" >&2
                usage
                exit 1
            fi
            ;;
    esac
    shift
done

for n in "$REPEAT_100K" "$REPEAT_1M" "$REPEAT_10M"; do
    if ! is_uint "$n"; then
        echo "[ERROR] repeat count must be a non-negative integer: $n" >&2
        exit 1
    fi
done

mkdir -p "$ROOT_DIR/results"
if [[ "$OUTFILE" != /* ]]; then
    OUTFILE="$ROOT_DIR/$OUTFILE"
fi
mkdir -p "$(dirname "$OUTFILE")"

cd "$ROOT_DIR"

echo "[INFO] variant=$VARIANT" | tee "$OUTFILE"
echo "[INFO] dev=$DEV" | tee -a "$OUTFILE"
echo "[INFO] repeats: 100K=$REPEAT_100K 1M=$REPEAT_1M 10M=$REPEAT_10M" | tee -a "$OUTFILE"
echo "[INFO] outfile=$OUTFILE" | tee -a "$OUTFILE"
echo "" | tee -a "$OUTFILE"

ensure_binary() {
    local variant="$1"
    local target="build/bin/cow-bench-$variant"
    if [[ ! -x "$target" ]]; then
        echo "[INFO] benchmark binary not found: $target" | tee -a "$OUTFILE"
        echo "[INFO] building: make bench-$variant" | tee -a "$OUTFILE"
        make "bench-$variant"
    fi
}

run_group() {
    local keys="$1"
    local label="$2"
    local repeat="$3"

    if (( repeat == 0 )); then
        echo "[$label] skipped (repeat=0)" | tee -a "$OUTFILE"
        echo "" | tee -a "$OUTFILE"
        return
    fi

    echo "[$label] keys=$keys repeat=$repeat" | tee -a "$OUTFILE"

    local sum_all=0
    local cnt_all=0
    local t
    for t in "${THREADS[@]}"; do
        eval "sum_${t}=0"
        eval "cnt_${t}=0"
    done

    local run
    for ((run = 1; run <= repeat; run++)); do
        echo "[RUN $run/$repeat] make run-$VARIANT $keys 0 $DEV" | tee -a "$OUTFILE"

        local out
        if ! out=$(make "run-$VARIANT" "$keys" 0 "$DEV" 2>&1); then
            printf "%s\n" "$out" | tee -a "$OUTFILE"
            echo "[ERROR] run failed: label=$label run=$run" | tee -a "$OUTFILE"
            exit 1
        fi

        printf "%s\n" "$out" | tee -a "$OUTFILE"

        while read -r thread thr; do
            if [[ -n "$thread" && -n "$thr" ]]; then
                eval "sum_${thread}=\$(echo \"\${sum_${thread}} + $thr\" | bc)"
                eval "cnt_${thread}=\$((\${cnt_${thread}} + 1))"
                sum_all=$(echo "$sum_all + $thr" | bc)
                cnt_all=$((cnt_all + 1))
            fi
        done < <(
            printf "%s\n" "$out" | awk '
                /^Threads:/ { t=$2 }
                /^Average throughput:/ {
                    if (t != "" && $3 ~ /^[0-9]+(\.[0-9]+)?$/) {
                        print t, $3
                    }
                }
            '
        )

        echo "" | tee -a "$OUTFILE"
    done

    printf "%-12s" "threads" | tee -a "$OUTFILE"
    for t in "${THREADS[@]}"; do
        printf "%12s" "$t" | tee -a "$OUTFILE"
    done
    echo "" | tee -a "$OUTFILE"

    printf "%-12s" "avg_ops/sec" | tee -a "$OUTFILE"
    for t in "${THREADS[@]}"; do
        eval "s=\${sum_${t}}"
        eval "c=\${cnt_${t}}"
        if (( c > 0 )); then
            avg=$(echo "scale=2; $s / $c" | bc)
            printf "%12.0f" "$avg" | tee -a "$OUTFILE"
        else
            printf "%12s" "N/A" | tee -a "$OUTFILE"
        fi
    done
    echo "" | tee -a "$OUTFILE"

    if (( cnt_all > 0 )); then
        all_avg=$(echo "scale=2; $sum_all / $cnt_all" | bc)
        echo "overall_avg(all thread samples): $(printf "%.2f" "$all_avg") ops/sec" | tee -a "$OUTFILE"
    else
        echo "overall_avg(all thread samples): N/A" | tee -a "$OUTFILE"
    fi

    echo "" | tee -a "$OUTFILE"
}

ensure_binary "$VARIANT"
run_group 100000 "100K" "$REPEAT_100K"
run_group 1000000 "1M" "$REPEAT_1M"
run_group 10000000 "10M" "$REPEAT_10M"

echo "Benchmark finished. Results saved to $OUTFILE" | tee -a "$OUTFILE"