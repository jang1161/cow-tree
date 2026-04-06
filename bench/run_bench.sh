#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DEFAULT_DEV="/dev/nvme3n2"
THREADS=(1 2 4 8 16 32 64)

REPEAT_100K=3
REPEAT_1M=3
REPEAT_10M=1
DEV="${DEV:-${DEVICE:-$DEFAULT_DEV}}"
OUTFILE="${OUTFILE:-}"

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

DEFAULT_OUTFILE="results/${VARIANT}_$(date +%m%d%H%M).txt"
if [[ -z "$OUTFILE" ]]; then
    OUTFILE="$DEFAULT_OUTFILE"
fi

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

: > "$OUTFILE"

echo "[INFO] sudo 권한 확인 중..."
sudo -v

# Long benchmarks can outlive sudo timestamp; refresh it in background.
keep_sudo_alive() {
    while true; do
        sudo -n true 2>/dev/null || break
        sleep 50
    done
}

keep_sudo_alive &
SUDO_KEEPALIVE_PID=$!

CURRENT_RUN_PID=""
CURRENT_FIFO=""

cleanup() {
    if [[ -n "${SUDO_KEEPALIVE_PID:-}" ]]; then
        kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
    fi
}

handle_interrupt() {
    echo ""
    echo "[INFO] interrupted; stopping benchmark..."

    if [[ -n "${CURRENT_RUN_PID:-}" ]]; then
        kill "$CURRENT_RUN_PID" 2>/dev/null || true
        pkill -TERM -P "$CURRENT_RUN_PID" 2>/dev/null || true
    fi

    if [[ -n "${CURRENT_FIFO:-}" ]]; then
        rm -f "$CURRENT_FIFO" 2>/dev/null || true
    fi

    cleanup
    exit 130
}

trap cleanup EXIT
trap handle_interrupt INT TERM

ensure_binary() {
    local variant="$1"
    local target="build/bin/cow-bench-$variant"
    if [[ ! -x "$target" ]]; then
        echo "[INFO] benchmark binary not found: $target"
        echo "[INFO] building: make bench-$variant"
        make "bench-$variant"
    fi
}

run_group() {
    local keys="$1"
    local label="$2"
    local repeat="$3"
    local bench_bin="./build/bin/cow-bench-$VARIANT"

    if (( repeat == 0 )); then
        echo "$label skipped (repeat=0)"
        echo "$label skipped (repeat=0)" >> "$OUTFILE"
        echo "" >> "$OUTFILE"
        return
    fi

    echo "$label keys=$keys repeat=$repeat"

    local t
    for t in "${THREADS[@]}"; do
        eval "sum_${t}=0"
        eval "cnt_${t}=0"
    done

    echo "$label" >> "$OUTFILE"

    printf "%-12s" "threads" >> "$OUTFILE"
    for t in "${THREADS[@]}"; do
        printf "%10s" "$t" >> "$OUTFILE"
    done
    echo "" >> "$OUTFILE"

    local run
    for ((run = 1; run <= repeat; run++)); do
        echo "[RUN $run/$repeat] sudo $bench_bin $keys 0 $DEV"

        printf "%-12s" "Run#$run" >> "$OUTFILE"

        local parsed_count=0
        local current_thread=""
        local fifo
        local make_pid
        fifo=$(mktemp -u)
        mkfifo "$fifo"
        CURRENT_FIFO="$fifo"

        if command -v stdbuf >/dev/null 2>&1; then
            sudo stdbuf -oL -eL "$bench_bin" "$keys" 0 "$DEV" > "$fifo" 2>&1 &
        else
            sudo "$bench_bin" "$keys" 0 "$DEV" > "$fifo" 2>&1 &
        fi
        make_pid=$!
        CURRENT_RUN_PID="$make_pid"

        while IFS= read -r line; do
            if [[ "$line" =~ ^Threads:[[:space:]]*([0-9]+) ]]; then
                current_thread="${BASH_REMATCH[1]}"
            elif [[ "$line" =~ ^Average\ throughput:[[:space:]]*([0-9]+([.][0-9]+)?) ]]; then
                local thr="${BASH_REMATCH[1]}"
                if [[ -n "$current_thread" ]]; then
                    printf "%10.0f" "$thr" >> "$OUTFILE"
                    parsed_count=$((parsed_count + 1))

                    eval "sum_${current_thread}=\$(echo \"\${sum_${current_thread}} + $thr\" | bc)"
                    eval "cnt_${current_thread}=\$((\${cnt_${current_thread}} + 1))"
                    current_thread=""
                fi
            fi
        done < "$fifo"

        rm -f "$fifo"
        CURRENT_FIFO=""

        if ! wait "$make_pid"; then
            echo "" >> "$OUTFILE"
            echo "[ERROR] run failed: label=$label run=$run" >&2
            exit 1
        fi

        CURRENT_RUN_PID=""

        while (( parsed_count < ${#THREADS[@]} )); do
            printf "%10s" "ERR" >> "$OUTFILE"
            parsed_count=$((parsed_count + 1))
        done

        echo "" >> "$OUTFILE"
    done

    printf "%-12s" "Average" >> "$OUTFILE"
    for t in "${THREADS[@]}"; do
        eval "s=\${sum_${t}}"
        eval "c=\${cnt_${t}}"
        if (( c > 0 )); then
            avg=$(echo "scale=2; $s / $c" | bc)
            printf "%10.0f" "$avg" >> "$OUTFILE"
        else
            printf "%10s" "N/A" >> "$OUTFILE"
        fi
    done
    echo "" >> "$OUTFILE"

    echo "" >> "$OUTFILE"
}

ensure_binary "$VARIANT"
run_group 100000 "100K" "$REPEAT_100K"
run_group 1000000 "1M" "$REPEAT_1M"
run_group 10000000 "10M" "$REPEAT_10M"

echo "Benchmark finished. Results saved to $OUTFILE"