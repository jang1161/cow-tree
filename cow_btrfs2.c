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

#include "cow_btrfs2.h"

#define DATA_ZONE_START 2
#define QUEUE_SHARDS 8
#define BATCH_MAX 64
#define MAP_INIT_CAP 65536
#define PAGE_SIZE_BYTES 4096
#define LOG_MAGIC 0x4254524653324C47ULL

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

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cv;
    insert_req *head;
    insert_req *tail;
    pthread_t tid;
} queue_shard;

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

    queue_shard shards[QUEUE_SHARDS];
    _Atomic(int) stop;

    pthread_mutex_t map_lock;
    kv_slot *map;
    size_t map_cap;
    size_t map_used;

    _Atomic(uint64_t) log_seq;
};

typedef struct {
    cow_tree *t;
    int sid;
} worker_ctx;

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

static void apply_and_flush_batch(cow_tree *t, insert_req **arr, int n)
{
    for (int i = 0; i < n; i++) {
        map_insert(t, arr[i]->key, arr[i]->value);
        append_log_record(t, arr[i]->key, arr[i]->value);
        complete_req(arr[i]);
    }
}

static void *worker_main(void *arg)
{
    worker_ctx *ctx = (worker_ctx *)arg;
    cow_tree *t = ctx->t;
    queue_shard *s = &t->shards[ctx->sid];
    free(ctx);

    insert_req *batch[BATCH_MAX];

    for (;;) {
        int n = 0;

        pthread_mutex_lock(&s->lock);
        while (!atomic_load_explicit(&t->stop, memory_order_acquire) && !s->head)
            pthread_cond_wait(&s->cv, &s->lock);

        if (atomic_load_explicit(&t->stop, memory_order_acquire) && !s->head) {
            pthread_mutex_unlock(&s->lock);
            break;
        }

        while (n < BATCH_MAX && s->head) {
            insert_req *r = s->head;
            s->head = r->next;
            if (!s->head)
                s->tail = NULL;
            r->next = NULL;
            batch[n++] = r;
        }
        pthread_mutex_unlock(&s->lock);

        if (n > 0)
            apply_and_flush_batch(t, batch, n);
    }

    return NULL;
}

cow_tree *cow_open(const char *path)
{
    cow_tree *t = (cow_tree *)calloc(1, sizeof(*t));
    if (!t)
        return NULL;

    if (pthread_mutex_init(&t->map_lock, NULL) != 0) {
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

    for (int i = 0; i < QUEUE_SHARDS; i++) {
        if (pthread_mutex_init(&t->shards[i].lock, NULL) != 0 ||
            pthread_cond_init(&t->shards[i].cv, NULL) != 0) {
            perror("pthread init shard");
            exit(EXIT_FAILURE);
        }

        worker_ctx *ctx = (worker_ctx *)malloc(sizeof(*ctx));
        if (!ctx) {
            perror("malloc worker ctx");
            exit(EXIT_FAILURE);
        }
        ctx->t = t;
        ctx->sid = i;

        if (pthread_create(&t->shards[i].tid, NULL, worker_main, ctx) != 0) {
            perror("pthread_create worker");
            exit(EXIT_FAILURE);
        }
    }

    return t;
}

void cow_close(cow_tree *t)
{
    if (!t)
        return;

    atomic_store_explicit(&t->stop, 1, memory_order_release);

    for (int i = 0; i < QUEUE_SHARDS; i++) {
        pthread_mutex_lock(&t->shards[i].lock);
        pthread_cond_broadcast(&t->shards[i].cv);
        pthread_mutex_unlock(&t->shards[i].lock);
    }

    for (int i = 0; i < QUEUE_SHARDS; i++) {
        pthread_join(t->shards[i].tid, NULL);
        pthread_mutex_destroy(&t->shards[i].lock);
        pthread_cond_destroy(&t->shards[i].cv);
    }

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

    uint64_t h = hash_u64((uint64_t)(uintptr_t)pthread_self() ^ (uint64_t)key);
    int sid = (int)(h % QUEUE_SHARDS);
    queue_shard *s = &t->shards[sid];

    pthread_mutex_lock(&s->lock);
    if (s->tail)
        s->tail->next = req;
    else
        s->head = req;
    s->tail = req;
    pthread_cond_signal(&s->cv);
    pthread_mutex_unlock(&s->lock);

    pthread_mutex_lock(&req->done_lock);
    while (!req->done)
        pthread_cond_wait(&req->done_cv, &req->done_lock);
    pthread_mutex_unlock(&req->done_lock);
}
