#define _POSIX_C_SOURCE 200809L

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <time.h>
#include <unistd.h>
#include "cow_v3.h"

#define PROGRESS_STEP 1000

typedef struct {
    cow_tree *t;
    int *keys;
    int start;
    int end;
} thread_arg;

atomic_int global_done = 0;

int *all_keys = NULL;
int total_keys = 0;   // ← 추가

void *worker(void *arg)
{
    thread_arg *a = (thread_arg *)arg;

    for (int i = a->start; i < a->end; i++) {

        int key = a->keys[i];

        char buf[120];
        sprintf(buf, "value-%d", key);

        cow_insert(a->t, key, buf);
    }

    return NULL;
}

void run_test(const char *dev_path, int num_threads)
{
    printf("\n===== Running with %d threads =====\n", num_threads);

    atomic_store(&global_done, 0);

    cow_tree *t = cow_open(dev_path);
    if (!t) {
        perror("cow_open failed");
        exit(1);
    }

    pthread_t *threads = malloc(sizeof(pthread_t) * num_threads);
    thread_arg *args = malloc(sizeof(thread_arg) * num_threads);

    int chunk = total_keys / num_threads;

    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    for (int i = 0; i < num_threads; i++) {
        args[i].t = t;
        args[i].keys = all_keys;
        args[i].start = i * chunk;
        args[i].end = (i == num_threads - 1)
                      ? total_keys
                      : (i + 1) * chunk;

        pthread_create(&threads[i], NULL, worker, &args[i]);
    }

    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &end);

    double elapsed =
        (end.tv_sec - start.tv_sec) +
        (end.tv_nsec - start.tv_nsec) / 1e9;

    double iops = total_keys / elapsed;

    printf("\nThreads: %d\n", num_threads);
    printf("Elapsed time: %.6f seconds\n", elapsed);
    printf("Average throughput: %.2f ops/sec\n", iops);

    cow_close(t);

    free(threads);
    free(args);

    printf("Resetting ZNS device...\n");
    char cmd[256];
    sprintf(cmd, "sudo nvme zns reset-zone -a %s", dev_path);
    system(cmd);

    sleep(1);
}

int main(int argc, char *argv[])
{
    if (argc != 4) {
        printf("Usage: %s <key_num> <mode> <device>\n", argv[0]);
        printf("mode: 0=all  1=1thread  2=2thread  4=4thread  8  16 32\n");
        return 1;
    }

    total_keys = atoi(argv[1]);   // ← key 개수
    int mode = atoi(argv[2]);
    const char *dev_path = argv[3];

    printf("Resetting ZNS device...\n");
    char cmd[256];
    sprintf(cmd, "sudo nvme zns reset-zone -a %s", dev_path);
    system(cmd);

    all_keys = malloc(sizeof(int) * total_keys);
    if (!all_keys) {
        perror("malloc keys failed");
        return 1;
    }

    for (int i = 0; i < total_keys; i++)
        all_keys[i] = i;

    srand(54321);

    for (int i = total_keys - 1; i > 0; i--) {
        int j = rand() % (i + 1);
        int tmp = all_keys[i];
        all_keys[i] = all_keys[j];
        all_keys[j] = tmp;
    }

    printf("Random key set generated (%d keys).\n", total_keys);

    int thread_counts[] = {1, 2, 4, 8, 16, 32, 64};
    int num_configs = sizeof(thread_counts) / sizeof(thread_counts[0]);

    if (mode == 0) {
        for (int i = 0; i < num_configs; i++) {
            run_test(dev_path, thread_counts[i]);
        }
    } else {
        int valid = 0;

        for (int i = 0; i < num_configs; i++) {
            if (mode == thread_counts[i]) {
                run_test(dev_path, mode);
                valid = 1;
                break;
            }
        }

        if (!valid) {
            printf("Invalid mode: %d\n", mode);
            printf("Allowed values: 0, 1, 2, 4, 8, 16\n");
            free(all_keys);
            return 1;
        }
    }

    free(all_keys);

    printf("\nAll tests completed.\n");
    return 0;
}