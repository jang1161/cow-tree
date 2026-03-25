#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <libnvme.h>
#include <libzbd/zbd.h>

#include "cow_btrfs1.h"

#define DATA_ZONE_START 2
#define BATCH_MAX 64
#define COMMIT_THREADS 2
#define MAP_INIT_CAP 65536
#define PAGE_SIZE_BYTES 4096
#define LOG_MAGIC 0x4254524653314C47ULL

typedef struct insert_req {
    int64_t key;
    char value[120];
    int done;
    pthread_mutex_t done_lock;
    pthread_cond_t done_cv;
    struct insert_req *next;
} insert_req;

typedef struct {
    uint8_t used;
    int64_t key;
    record value;
} kv_slot;

typedef enum {
    TX_RUNNING = 0,
    TX_COMMIT_PREP,
    TX_COMMIT_DOING,
    TX_COMPLETE,
} tx_state;

typedef struct tx_batch {
    insert_req *items[BATCH_MAX];
    int n;
    _Atomic(int) state;
    _Atomic(int) winner;
} tx_batch;

typedef struct {
    uint64_t magic;
    uint64_t seq;
    int64_t key;
    char value[120];
    uint8_t pad[PAGE_SIZE_BYTES - 8 - 8 - 8 - 120];
} log_page;

struct cow_tree {
    int fd;
    __u32 nsid;
    struct zbd_info info;
    struct zbd_zone *zones;

    _Atomic(uint32_t) current_zone;
    _Atomic(uint64_t) *zone_wp_bytes;
    _Atomic(uint8_t) *zone_full;

    pthread_mutex_t q_lock;
    pthread_cond_t q_cv;
    insert_req *q_head;
    insert_req *q_tail;

    pthread_t sync_tid;
    _Atomic(int) stop_sync;

    pthread_t commit_tids[COMMIT_THREADS];
    _Atomic(int) stop_commit;

    pthread_mutex_t tx_lock;
    pthread_cond_t tx_cv;
    tx_batch *active_tx;

    pthread_mutex_t map_lock;
    kv_slot *map;
    size_t map_cap;
    size_t map_used;

    _Atomic(uint64_t) log_seq;
};

static pthread_key_t req_tls_key;
static pthread_once_t req_tls_once = PTHREAD_ONCE_INIT;

static inline uint64_t hash_u64(uint64_t x)
{
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

static void free_tls_req(void *ptr)
{
    insert_req *req = (insert_req *)ptr;
    if (!req)
        return;
    pthread_cond_destroy(&req->done_cv);
    pthread_mutex_destroy(&req->done_lock);
    free(req);
}

static void init_req_tls(void)
{
    if (pthread_key_create(&req_tls_key, free_tls_req) != 0) {
        perror("pthread_key_create");
        exit(EXIT_FAILURE);
    }
}

static insert_req *get_tls_req(void)
{
    if (pthread_once(&req_tls_once, init_req_tls) != 0) {
        perror("pthread_once");
        exit(EXIT_FAILURE);
    }

    insert_req *req = (insert_req *)pthread_getspecific(req_tls_key);
    if (req)
        return req;

    req = (insert_req *)calloc(1, sizeof(*req));
    if (!req) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }
    if (pthread_mutex_init(&req->done_lock, NULL) != 0) {
        perror("pthread_mutex_init");
        exit(EXIT_FAILURE);
    }
    if (pthread_cond_init(&req->done_cv, NULL) != 0) {
        perror("pthread_cond_init");
        exit(EXIT_FAILURE);
    }
    if (pthread_setspecific(req_tls_key, req) != 0) {
        perror("pthread_setspecific");
        exit(EXIT_FAILURE);
    }
    return req;
}

static void map_init(cow_tree *t)
{
    t->map_cap = MAP_INIT_CAP;
    t->map = (kv_slot *)calloc(t->map_cap, sizeof(*t->map));
    if (!t->map) {
        perror("calloc map");
        exit(EXIT_FAILURE);
    }
}

static void map_grow(cow_tree *t)
{
    size_t old_cap = t->map_cap;
    kv_slot *old_map = t->map;
    size_t new_cap = old_cap << 1;
    kv_slot *new_map = (kv_slot *)calloc(new_cap, sizeof(*new_map));
    if (!new_map) {
        perror("calloc map grow");
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < old_cap; i++) {
        if (!old_map[i].used)
            continue;
        size_t pos = (size_t)(hash_u64((uint64_t)old_map[i].key) & (new_cap - 1));
        while (new_map[pos].used)
            pos = (pos + 1) & (new_cap - 1);
        new_map[pos] = old_map[i];
    }

    free(old_map);
    t->map = new_map;
    t->map_cap = new_cap;
}

static void map_insert(cow_tree *t, int64_t key, const char *value)
{
    pthread_mutex_lock(&t->map_lock);
    if ((t->map_used + 1) * 10 >= t->map_cap * 7)
        map_grow(t);

    size_t pos = (size_t)(hash_u64((uint64_t)key) & (t->map_cap - 1));
    for (;;) {
        kv_slot *s = &t->map[pos];
        if (!s->used) {
            s->used = 1;
            s->key = key;
            memcpy(s->value.value, value, sizeof(s->value.value));
            t->map_used++;
            break;
        }
        if (s->key == key) {
            memcpy(s->value.value, value, sizeof(s->value.value));
            break;
        }
        pos = (pos + 1) & (t->map_cap - 1);
    }
    pthread_mutex_unlock(&t->map_lock);
}

static record *map_find(cow_tree *t, int64_t key)
{
    record *out = NULL;
    pthread_mutex_lock(&t->map_lock);
    size_t pos = (size_t)(hash_u64((uint64_t)key) & (t->map_cap - 1));
    for (;;) {
        kv_slot *s = &t->map[pos];
        if (!s->used)
            break;
        if (s->key == key) {
            out = (record *)malloc(sizeof(*out));
            if (out)
                *out = s->value;
            break;
        }
        pos = (pos + 1) & (t->map_cap - 1);
    }
    pthread_mutex_unlock(&t->map_lock);
    return out;
}

static inline uint32_t zone_next(cow_tree *t, uint32_t zone_id)
{
    return (zone_id + 1 < t->info.nr_zones) ? (zone_id + 1) : t->info.nr_zones;
}

static uint32_t reserve_writable_zone(cow_tree *t)
{
    for (;;) {
        uint32_t cur = atomic_load_explicit(&t->current_zone, memory_order_acquire);
        if (cur < DATA_ZONE_START) {
            uint32_t expected = cur;
            atomic_compare_exchange_weak_explicit(&t->current_zone, &expected,
                                                  DATA_ZONE_START,
                                                  memory_order_acq_rel,
                                                  memory_order_acquire);
            cur = DATA_ZONE_START;
        }

        if (cur >= t->info.nr_zones) {
            fprintf(stderr, "zones exhausted\n");
            exit(EXIT_FAILURE);
        }

        if (atomic_load_explicit(&t->zone_full[cur], memory_order_acquire)) {
            uint32_t expected = cur;
            uint32_t next = zone_next(t, cur);
            atomic_compare_exchange_weak_explicit(&t->current_zone, &expected, next,
                                                  memory_order_acq_rel,
                                                  memory_order_acquire);
            continue;
        }

        return cur;
    }
}

static int zone_append_raw_nolock(cow_tree *t, uint32_t zone_id, const void *buf,
                                  uint64_t *out_wp_bytes)
{
    __u64 zslba = t->zones[zone_id].start / t->info.lblock_size;
    __u16 nlb = (PAGE_SIZE_BYTES / t->info.lblock_size) - 1;
    __u64 result = 0;

    struct nvme_zns_append_args args = {
        .zslba = zslba,
        .result = &result,
        .data = (void *)buf,
        .metadata = NULL,
        .args_size = sizeof(args),
        .fd = t->fd,
        .timeout = 0,
        .nsid = t->nsid,
        .ilbrt = 0,
        .data_len = PAGE_SIZE_BYTES,
        .metadata_len = 0,
        .nlb = nlb,
        .control = 0,
        .lbat = 0,
        .lbatm = 0,
        .ilbrt_u64 = 0,
    };

    if (nvme_zns_append(&args) != 0)
        return -1;

    if (out_wp_bytes)
        *out_wp_bytes = (result + nlb + 1) * t->info.lblock_size;

    return 0;
}

static void append_log_record(cow_tree *t, int64_t key, const char *value)
{
    log_page p;
    memset(&p, 0, sizeof(p));
    p.magic = LOG_MAGIC;
    p.seq = atomic_fetch_add_explicit(&t->log_seq, 1, memory_order_relaxed) + 1;
    p.key = key;
    memcpy(p.value, value, sizeof(p.value));

    const uint64_t page_bytes = PAGE_SIZE_BYTES;

    for (;;) {
        uint32_t zid = reserve_writable_zone(t);
        uint64_t old_wp = atomic_fetch_add_explicit(&t->zone_wp_bytes[zid], page_bytes,
                                                    memory_order_acq_rel);
        uint64_t zone_end = t->zones[zid].start + t->zones[zid].capacity;

        if (old_wp + page_bytes > zone_end) {
            atomic_fetch_sub_explicit(&t->zone_wp_bytes[zid], page_bytes, memory_order_acq_rel);
            atomic_store_explicit(&t->zone_full[zid], 1, memory_order_release);
            uint32_t expected = zid;
            uint32_t next = zone_next(t, zid);
            atomic_compare_exchange_weak_explicit(&t->current_zone, &expected, next,
                                                  memory_order_acq_rel,
                                                  memory_order_acquire);
            continue;
        }

        uint64_t wp_bytes;
        if (zone_append_raw_nolock(t, zid, &p, &wp_bytes) == 0) {
            uint64_t cur_wp = atomic_load_explicit(&t->zone_wp_bytes[zid], memory_order_acquire);
            while (wp_bytes > cur_wp &&
                   !atomic_compare_exchange_weak_explicit(&t->zone_wp_bytes[zid], &cur_wp, wp_bytes,
                                                         memory_order_acq_rel,
                                                         memory_order_acquire)) {
            }
            if (wp_bytes >= zone_end) {
                atomic_store_explicit(&t->zone_full[zid], 1, memory_order_release);
                uint32_t expected = zid;
                uint32_t next = zone_next(t, zid);
                atomic_compare_exchange_weak_explicit(&t->current_zone, &expected, next,
                                                      memory_order_acq_rel,
                                                      memory_order_acquire);
            }
            return;
        }

        atomic_store_explicit(&t->zone_full[zid], 1, memory_order_release);
        uint32_t expected = zid;
        uint32_t next = zone_next(t, zid);
        atomic_compare_exchange_weak_explicit(&t->current_zone, &expected, next,
                                              memory_order_acq_rel,
                                              memory_order_acquire);
    }
}

static void complete_req(insert_req *req)
{
    pthread_mutex_lock(&req->done_lock);
    req->done = 1;
    pthread_cond_signal(&req->done_cv);
    pthread_mutex_unlock(&req->done_lock);
}

static int pop_batch(cow_tree *t, insert_req **batch)
{
    int n = 0;
    pthread_mutex_lock(&t->q_lock);
    while (!atomic_load_explicit(&t->stop_sync, memory_order_acquire) && t->q_head == NULL)
        pthread_cond_wait(&t->q_cv, &t->q_lock);

    if (atomic_load_explicit(&t->stop_sync, memory_order_acquire) && t->q_head == NULL) {
        pthread_mutex_unlock(&t->q_lock);
        return 0;
    }

    while (n < BATCH_MAX && t->q_head) {
        insert_req *r = t->q_head;
        t->q_head = r->next;
        if (!t->q_head)
            t->q_tail = NULL;
        r->next = NULL;
        batch[n++] = r;
    }
    pthread_mutex_unlock(&t->q_lock);
    return n;
}

static void commit_tx_batch(cow_tree *t, tx_batch *tx)
{
    for (int i = 0; i < tx->n; i++) {
        map_insert(t, tx->items[i]->key, tx->items[i]->value);
        append_log_record(t, tx->items[i]->key, tx->items[i]->value);
        complete_req(tx->items[i]);
    }
}

static void *commit_main(void *arg)
{
    cow_tree *t = (cow_tree *)arg;
    int tid = -1;

    for (int i = 0; i < COMMIT_THREADS; i++) {
        if (pthread_equal(pthread_self(), t->commit_tids[i])) {
            tid = i;
            break;
        }
    }

    for (;;) {
        tx_batch *tx = NULL;

        pthread_mutex_lock(&t->tx_lock);
        while (!t->active_tx && !atomic_load_explicit(&t->stop_commit, memory_order_acquire))
            pthread_cond_wait(&t->tx_cv, &t->tx_lock);

        if (!t->active_tx && atomic_load_explicit(&t->stop_commit, memory_order_acquire)) {
            pthread_mutex_unlock(&t->tx_lock);
            break;
        }

        tx = t->active_tx;
        pthread_mutex_unlock(&t->tx_lock);

        int expected = -1;
        if (atomic_compare_exchange_strong_explicit(&tx->winner, &expected, tid,
                                                    memory_order_acq_rel,
                                                    memory_order_acquire)) {
            atomic_store_explicit(&tx->state, TX_COMMIT_DOING, memory_order_release);
            commit_tx_batch(t, tx);
            atomic_store_explicit(&tx->state, TX_COMPLETE, memory_order_release);

            pthread_mutex_lock(&t->tx_lock);
            pthread_cond_broadcast(&t->tx_cv);
            pthread_mutex_unlock(&t->tx_lock);
        } else {
            pthread_mutex_lock(&t->tx_lock);
            while (t->active_tx == tx &&
                   atomic_load_explicit(&tx->state, memory_order_acquire) != TX_COMPLETE &&
                   !atomic_load_explicit(&t->stop_commit, memory_order_acquire)) {
                pthread_cond_wait(&t->tx_cv, &t->tx_lock);
            }
            pthread_mutex_unlock(&t->tx_lock);
        }
    }

    return NULL;
}

static void *sync_main(void *arg)
{
    cow_tree *t = (cow_tree *)arg;
    insert_req *batch[BATCH_MAX];

    for (;;) {
        int n = pop_batch(t, batch);
        if (n == 0)
            break;

        tx_batch *tx = (tx_batch *)calloc(1, sizeof(*tx));
        if (!tx) {
            perror("calloc tx_batch");
            exit(EXIT_FAILURE);
        }

        tx->n = n;
        for (int i = 0; i < n; i++)
            tx->items[i] = batch[i];
        atomic_store_explicit(&tx->state, TX_RUNNING, memory_order_release);
        atomic_store_explicit(&tx->winner, -1, memory_order_release);

        pthread_mutex_lock(&t->tx_lock);
        while (t->active_tx)
            pthread_cond_wait(&t->tx_cv, &t->tx_lock);

        t->active_tx = tx;
        atomic_store_explicit(&tx->state, TX_COMMIT_PREP, memory_order_release);
        pthread_cond_broadcast(&t->tx_cv);

        while (atomic_load_explicit(&tx->state, memory_order_acquire) != TX_COMPLETE)
            pthread_cond_wait(&t->tx_cv, &t->tx_lock);

        t->active_tx = NULL;
        pthread_cond_broadcast(&t->tx_cv);
        pthread_mutex_unlock(&t->tx_lock);

        free(tx);
    }

    return NULL;
}

cow_tree *cow_open(const char *path)
{
    cow_tree *t = (cow_tree *)calloc(1, sizeof(*t));
    if (!t)
        return NULL;

    if (pthread_mutex_init(&t->q_lock, NULL) != 0 ||
        pthread_cond_init(&t->q_cv, NULL) != 0 ||
        pthread_mutex_init(&t->tx_lock, NULL) != 0 ||
        pthread_cond_init(&t->tx_cv, NULL) != 0 ||
        pthread_mutex_init(&t->map_lock, NULL) != 0) {
        free(t);
        return NULL;
    }

    t->fd = zbd_open(path, O_RDWR, &t->info);
    if (t->fd < 0) {
        perror("zbd_open");
        free(t);
        return NULL;
    }

    if (nvme_get_nsid(t->fd, &t->nsid) != 0) {
        perror("nvme_get_nsid");
        zbd_close(t->fd);
        free(t);
        return NULL;
    }

    t->zones = (struct zbd_zone *)calloc(t->info.nr_zones, sizeof(*t->zones));
    t->zone_wp_bytes = (_Atomic(uint64_t) *)calloc(t->info.nr_zones, sizeof(*t->zone_wp_bytes));
    t->zone_full = (_Atomic(uint8_t) *)calloc(t->info.nr_zones, sizeof(*t->zone_full));
    if (!t->zones || !t->zone_wp_bytes || !t->zone_full) {
        perror("calloc zones");
        zbd_close(t->fd);
        free(t->zones);
        free(t->zone_wp_bytes);
        free(t->zone_full);
        free(t);
        return NULL;
    }

    unsigned int nr = t->info.nr_zones;
    if (zbd_report_zones(t->fd, 0, 0, ZBD_RO_ALL, t->zones, &nr) != 0) {
        perror("zbd_report_zones");
        zbd_close(t->fd);
        free(t->zones);
        free(t->zone_wp_bytes);
        free(t->zone_full);
        free(t);
        return NULL;
    }

    uint32_t initial = DATA_ZONE_START;
    for (uint32_t z = 0; z < t->info.nr_zones; z++) {
        atomic_store_explicit(&t->zone_wp_bytes[z], t->zones[z].wp, memory_order_relaxed);
        atomic_store_explicit(&t->zone_full[z],
                              (t->zones[z].cond == ZBD_ZONE_COND_FULL) ? 1 : 0,
                              memory_order_relaxed);
    }
    for (uint32_t z = DATA_ZONE_START; z < t->info.nr_zones; z++) {
        if (!atomic_load_explicit(&t->zone_full[z], memory_order_acquire)) {
            initial = z;
            break;
        }
    }
    atomic_store_explicit(&t->current_zone, initial, memory_order_release);

    map_init(t);

    if (pthread_create(&t->sync_tid, NULL, sync_main, t) != 0) {
        perror("pthread_create sync");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < COMMIT_THREADS; i++) {
        if (pthread_create(&t->commit_tids[i], NULL, commit_main, t) != 0) {
            perror("pthread_create commit");
            exit(EXIT_FAILURE);
        }
    }

    return t;
}

void cow_close(cow_tree *t)
{
    if (!t)
        return;

    atomic_store_explicit(&t->stop_sync, 1, memory_order_release);
    pthread_mutex_lock(&t->q_lock);
    pthread_cond_broadcast(&t->q_cv);
    pthread_mutex_unlock(&t->q_lock);
    pthread_join(t->sync_tid, NULL);

    atomic_store_explicit(&t->stop_commit, 1, memory_order_release);
    pthread_mutex_lock(&t->tx_lock);
    pthread_cond_broadcast(&t->tx_cv);
    pthread_mutex_unlock(&t->tx_lock);

    for (int i = 0; i < COMMIT_THREADS; i++)
        pthread_join(t->commit_tids[i], NULL);

    pthread_mutex_destroy(&t->q_lock);
    pthread_cond_destroy(&t->q_cv);
    pthread_mutex_destroy(&t->tx_lock);
    pthread_cond_destroy(&t->tx_cv);
    pthread_mutex_destroy(&t->map_lock);

    free(t->map);
    free(t->zones);
    free(t->zone_wp_bytes);
    free(t->zone_full);
    zbd_close(t->fd);
    free(t);
}

record *cow_find(cow_tree *t, int64_t key)
{
    if (!t)
        return NULL;
    return map_find(t, key);
}

void cow_insert(cow_tree *t, int64_t key, const char *value)
{
    insert_req *req = get_tls_req();
    req->key = key;
    memcpy(req->value, value, sizeof(req->value));
    req->done = 0;
    req->next = NULL;

    pthread_mutex_lock(&t->q_lock);
    if (t->q_tail)
        t->q_tail->next = req;
    else
        t->q_head = req;
    t->q_tail = req;
    pthread_cond_signal(&t->q_cv);
    pthread_mutex_unlock(&t->q_lock);

    pthread_mutex_lock(&req->done_lock);
    while (!req->done)
        pthread_cond_wait(&req->done_cv, &req->done_lock);
    pthread_mutex_unlock(&req->done_lock);
}
