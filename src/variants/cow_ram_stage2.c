#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "cow_ram_stage2.h"

#define MAX_HEIGHT 32
#define RIGHTMOST_IDX UINT32_MAX

#define MAX_BATCH_PAGES 2048
#define MAX_NVME_PAGES  64

#define TXG_BATCH_MAX 256
#define TXG_BATCH_WAIT_US 150
#define TXG_BATCH_MIN_WAIT_US 30
#define TXG_COMMIT_MERGE_JOBS 4
#define TXG_COMMIT_MERGE_ITEMS 512
#define PROF_SAMPLE_MASK 1023U

#define READ_CACHE_SLOTS 64
#define RAM_TABLE_INIT_CAP 65536
#define TEMP_NODE_BIT (1ULL << 63)

typedef uint64_t node_id_t;

typedef struct
{
    uint8_t valid;
    pagenum_t pn;
    page data;
} read_cache_entry;

typedef struct
{
    node_id_t id;
    page node;
    uint8_t dirty;
    uint8_t flushed;
    pagenum_t flushed_pn;
} overlay_node;

typedef struct
{
    cow_tree *t;
    overlay_node *arr;
    size_t len;
    size_t cap;
    uint64_t next_temp;
    size_t *idx_table;
    size_t idx_cap;
    size_t idx_used;
} overlay_state;

typedef struct
{
    node_id_t id;
    uint32_t cidx;
} overlay_path_entry;

typedef struct
{
    overlay_path_entry e[MAX_HEIGHT];
    int depth;
} overlay_path;

typedef struct
{
    overlay_node *n;
} batch_entry;

typedef struct
{
    insert_req *req;
    int ord;
} batch_item;

struct txg_batch_job
{
    uint64_t start_ns;
    int n;
    batch_item items[TXG_BATCH_MAX];
    struct txg_batch_job *next;
};

static pthread_key_t direct_read_buf_key;
static pthread_once_t direct_read_buf_key_once = PTHREAD_ONCE_INIT;
static pthread_key_t req_tls_key;
static pthread_once_t req_tls_key_once = PTHREAD_ONCE_INIT;
static __thread read_cache_entry tl_read_cache[READ_CACHE_SLOTS];
static __thread uint32_t prof_sample_ctr;

static inline uint64_t monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline int prof_should_sample(void)
{
    prof_sample_ctr++;
    return (prof_sample_ctr & PROF_SAMPLE_MASK) == 0;
}

static inline void stat_update_max_u64(_Atomic(uint64_t) *dst, uint64_t v)
{
    uint64_t cur = atomic_load_explicit(dst, memory_order_relaxed);
    while (v > cur &&
           !atomic_compare_exchange_weak_explicit(dst, &cur, v,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed))
    {
    }
}

typedef struct
{
    uint8_t used;
    pagenum_t pn;
    page *data;
} ram_page_slot;

static inline int is_temp_id(node_id_t id)
{
    return (id & TEMP_NODE_BIT) != 0;
}

static inline node_id_t make_temp_id(uint64_t n)
{
    return TEMP_NODE_BIT | n;
}

static inline read_cache_entry *read_cache_slot(pagenum_t pn)
{
    return &tl_read_cache[pn & (READ_CACHE_SLOTS - 1)];
}

static inline uint64_t page_hash_u64(uint64_t x)
{
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

static inline ram_page_slot *ram_slots(cow_tree *t)
{
    return (ram_page_slot *)t->ram_slots;
}

static void ram_table_init(cow_tree *t)
{
    t->ram_cap = RAM_TABLE_INIT_CAP;
    t->ram_used = 0;
    t->ram_slots = calloc(t->ram_cap, sizeof(ram_page_slot));
    if (!t->ram_slots)
    {
        perror("calloc ram table");
        exit(EXIT_FAILURE);
    }
}

static void ram_table_destroy(cow_tree *t)
{
    if (!t->ram_slots)
        return;

    ram_page_slot *slots = ram_slots(t);
    for (size_t i = 0; i < t->ram_cap; i++)
    {
        if (slots[i].used)
            free(slots[i].data);
    }
    free(t->ram_slots);
    t->ram_slots = NULL;
    t->ram_cap = 0;
    t->ram_used = 0;
}

static void ram_table_grow(cow_tree *t)
{
    size_t old_cap = t->ram_cap;
    ram_page_slot *old_slots = ram_slots(t);

    size_t new_cap = old_cap << 1;
    ram_page_slot *new_slots = calloc(new_cap, sizeof(ram_page_slot));
    if (!new_slots)
    {
        perror("calloc ram table grow");
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < old_cap; i++)
    {
        if (!old_slots[i].used)
            continue;

        size_t pos = (size_t)(page_hash_u64((uint64_t)old_slots[i].pn) & (new_cap - 1));
        while (new_slots[pos].used)
        {
            pos = (pos + 1) & (new_cap - 1);
        }
        new_slots[pos] = old_slots[i];
    }

    free(old_slots);
    t->ram_slots = new_slots;
    t->ram_cap = new_cap;
}

static int ram_table_lookup(cow_tree *t, pagenum_t pn, page *dst)
{
    if (!t->ram_slots)
        return 0;

    ram_page_slot *slots = ram_slots(t);
    size_t pos = (size_t)(page_hash_u64((uint64_t)pn) & (t->ram_cap - 1));

    for (;;)
    {
        ram_page_slot *s = &slots[pos];
        if (!s->used)
            return 0;
        if (s->pn == pn)
        {
            *dst = *s->data;
            return 1;
        }
        pos = (pos + 1) & (t->ram_cap - 1);
    }
}

static void ram_table_insert(cow_tree *t, pagenum_t pn, const page *src)
{
    if (!t->ram_slots)
        ram_table_init(t);

    if ((t->ram_used + 1) * 10 >= t->ram_cap * 7)
        ram_table_grow(t);

    ram_page_slot *slots = ram_slots(t);
    size_t pos = (size_t)(page_hash_u64((uint64_t)pn) & (t->ram_cap - 1));

    for (;;)
    {
        ram_page_slot *s = &slots[pos];
        if (!s->used)
        {
            s->data = malloc(sizeof(page));
            if (!s->data)
            {
                perror("malloc ram page");
                exit(EXIT_FAILURE);
            }
            *s->data = *src;
            s->pn = pn;
            s->used = 1;
            t->ram_used++;
            return;
        }

        if (s->pn == pn)
        {
            *s->data = *src;
            return;
        }

        pos = (pos + 1) & (t->ram_cap - 1);
    }
}

static void free_tls_buf(void *ptr)
{
    free(ptr);
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

static void init_direct_read_buf_key(void)
{
    if (pthread_key_create(&direct_read_buf_key, free_tls_buf) != 0)
    {
        perror("pthread_key_create");
        exit(EXIT_FAILURE);
    }
}

static void init_req_tls_key(void)
{
    if (pthread_key_create(&req_tls_key, free_tls_req) != 0)
    {
        perror("pthread_key_create req_tls_key");
        exit(EXIT_FAILURE);
    }
}

static void *get_tls_direct_read_buf(void)
{
    if (pthread_once(&direct_read_buf_key_once, init_direct_read_buf_key) != 0)
    {
        perror("pthread_once");
        exit(EXIT_FAILURE);
    }

    void *buf = pthread_getspecific(direct_read_buf_key);
    if (buf)
        return buf;

    if (posix_memalign(&buf, PAGE_SIZE, PAGE_SIZE) != 0)
    {
        perror("posix_memalign");
        exit(EXIT_FAILURE);
    }
    memset(buf, 0, PAGE_SIZE);

    if (pthread_setspecific(direct_read_buf_key, buf) != 0)
    {
        perror("pthread_setspecific");
        free(buf);
        exit(EXIT_FAILURE);
    }

    return buf;
}

static insert_req *get_tls_insert_req(void)
{
    if (pthread_once(&req_tls_key_once, init_req_tls_key) != 0)
    {
        perror("pthread_once req_tls_key");
        exit(EXIT_FAILURE);
    }

    insert_req *req = pthread_getspecific(req_tls_key);
    if (req)
    {
        return req;
    }

    req = calloc(1, sizeof(*req));
    if (!req)
    {
        perror("calloc tls insert_req");
        exit(EXIT_FAILURE);
    }

    if (pthread_mutex_init(&req->done_lock, NULL) != 0)
    {
        perror("pthread_mutex_init tls req.done_lock");
        free(req);
        exit(EXIT_FAILURE);
    }
    if (pthread_cond_init(&req->done_cv, NULL) != 0)
    {
        perror("pthread_cond_init tls req.done_cv");
        pthread_mutex_destroy(&req->done_lock);
        free(req);
        exit(EXIT_FAILURE);
    }

    if (pthread_setspecific(req_tls_key, req) != 0)
    {
        perror("pthread_setspecific req_tls_key");
        pthread_cond_destroy(&req->done_cv);
        pthread_mutex_destroy(&req->done_lock);
        free(req);
        exit(EXIT_FAILURE);
    }

    return req;
}

static int is_empty_snapshot(pagenum_t root_pn)
{
    return root_pn == INVALID_PGN;
}

static void read_tree_snapshot(cow_tree *t, pagenum_t *root_pn, uint64_t *seq_no)
{
    for (;;)
    {
        uint64_t s1 = atomic_load_explicit(&t->volatile_sb.seq_no, memory_order_acquire);
        if (s1 & 1ULL)
            continue;

        pagenum_t r = atomic_load_explicit(&t->volatile_sb.root_pn, memory_order_acquire);
        uint64_t s2 = atomic_load_explicit(&t->volatile_sb.seq_no, memory_order_acquire);
        if (s1 == s2 && ((s2 & 1ULL) == 0))
        {
            *root_pn = r;
            *seq_no = s2;
            return;
        }
    }
}

static inline uint32_t zone_next(cow_tree *t, uint32_t zone_id)
{
    return (zone_id + 1 < t->info.nr_zones) ? (zone_id + 1) : t->info.nr_zones;
}


static void load_page(cow_tree *t, pagenum_t pn, page *dst)
{
    off_t off = (off_t)pn * PAGE_SIZE;

    read_cache_entry *slot = read_cache_slot(pn);
    if (slot->valid && slot->pn == pn)
    {
        *dst = slot->data;
        atomic_fetch_add_explicit(&t->stat_tl_cache_hit, 1, memory_order_relaxed);
        return;
    }

    if (ram_table_lookup(t, pn, dst))
    {
        atomic_fetch_add_explicit(&t->stat_ram_cache_hit, 1, memory_order_relaxed);
        slot->valid = 1;
        slot->pn = pn;
        slot->data = *dst;
        return;
    }

    if (t->direct_fd >= 0)
    {
        void *raw = get_tls_direct_read_buf();
        ssize_t n = pread(t->direct_fd, raw, PAGE_SIZE, off);
        if (n != PAGE_SIZE)
        {
            perror("load_page(O_DIRECT)");
            exit(EXIT_FAILURE);
        }
        memcpy(dst, raw, PAGE_SIZE);
        atomic_fetch_add_explicit(&t->stat_disk_reads, 1, memory_order_relaxed);
    }
    else
    {
        if (pread(t->fd, dst, PAGE_SIZE, off) != PAGE_SIZE)
        {
            perror("load_page");
            exit(EXIT_FAILURE);
        }
        atomic_fetch_add_explicit(&t->stat_disk_reads, 1, memory_order_relaxed);
    }

    slot->valid = 1;
    slot->pn = pn;
    slot->data = *dst;
    ram_table_insert(t, pn, dst);
}

static int zone_append_raw_nolock(cow_tree *t, uint32_t zone_id, const void *buf,
                                  pagenum_t *out_pn, uint64_t *out_wp_bytes)
{
    __u64 zslba = t->zones[zone_id].start / t->info.lblock_size;
    __u16 nlb = (PAGE_SIZE / t->info.lblock_size) - 1;
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
        .data_len = PAGE_SIZE,
        .metadata_len = 0,
        .nlb = nlb,
        .control = 0,
        .lbat = 0,
        .lbatm = 0,
        .ilbrt_u64 = 0};

    if (nvme_zns_append(&args) != 0)
    {
        return -1;
    }

    if (out_wp_bytes)
        *out_wp_bytes = (result + nlb + 1) * t->info.lblock_size;
    if (out_pn)
        *out_pn = (pagenum_t)(result * t->info.lblock_size / PAGE_SIZE);
    return 0;
}


static uint64_t scan_meta_zone(int fd, uint32_t zone_id, uint64_t zone_size, superblock_entry *out)
{
    uint64_t zone_pages = zone_size / PAGE_SIZE;
    uint64_t last_wp = 0;

    for (uint64_t i = 1; i < zone_pages; i++)
    {
        superblock_entry tmp;
        off_t off = (off_t)zone_id * zone_size + (off_t)i * PAGE_SIZE;
        if (pread(fd, &tmp, PAGE_SIZE, off) != PAGE_SIZE)
            break;
        if (tmp.magic != SB_MAGIC)
            break;
        *out = tmp;
        last_wp = i;
    }
    return last_wp;
}

static void activate_meta_zone(cow_tree *t, uint32_t zone_id, uint64_t version)
{
    off_t zstart = (off_t)zone_id * t->info.zone_size;
    if (zbd_reset_zones(t->fd, zstart, (off_t)t->info.zone_size) != 0)
    {
        perror("zbd_reset_zones");
        exit(EXIT_FAILURE);
    }

    atomic_store_explicit(&t->zone_wp_bytes[zone_id], t->zones[zone_id].start, memory_order_release);
    atomic_store_explicit(&t->zone_full[zone_id], 0, memory_order_release);

    zone_header zh;
    memset(&zh, 0, sizeof zh);
    zh.magic = ZH_MAGIC;
    zh.state = ZH_ACTIVE;
    zh.version = version;

    pagenum_t ignored_pn;
    uint64_t wp_bytes;
    if (zone_append_raw_nolock(t, zone_id, &zh, &ignored_pn, &wp_bytes) != 0)
    {
        perror("nvme_zns_append(meta_zone_header)");
        exit(EXIT_FAILURE);
    }

    atomic_store_explicit(&t->zone_wp_bytes[zone_id], wp_bytes, memory_order_release);
    atomic_store_explicit(
        &t->zone_full[zone_id],
        (wp_bytes >= t->zones[zone_id].start + t->zones[zone_id].capacity) ? 1 : 0,
        memory_order_release);

    t->active_zone = zone_id;
    t->meta_wp = 1;
    t->version = version;
}

static void load_superblock(cow_tree *t)
{
    zone_header zh0, zh1;
    int v0 = (pread(t->fd, &zh0, PAGE_SIZE, 0) == PAGE_SIZE) && (zh0.magic == ZH_MAGIC);
    int v1 = (pread(t->fd, &zh1, PAGE_SIZE, (off_t)META_ZONE_1 * t->info.zone_size) == PAGE_SIZE) &&
             (zh1.magic == ZH_MAGIC);

    superblock_entry sb0, sb1;
    uint64_t wp0 = 0, wp1 = 0;

    if (v0)
        wp0 = scan_meta_zone(t->fd, META_ZONE_0, t->info.zone_size, &sb0);
    if (v1)
        wp1 = scan_meta_zone(t->fd, META_ZONE_1, t->info.zone_size, &sb1);

    if (wp0 == 0 && wp1 == 0)
    {
        memset(&t->durable_sb, 0, sizeof t->durable_sb);
        t->durable_sb.root_pn = INVALID_PGN;
        t->durable_sb.leaf_order = LEAF_ORDER;
        t->durable_sb.internal_order = INTERNAL_ORDER;
        t->durable_sb.seq_no = 0;
        activate_meta_zone(t, META_ZONE_0, 0);
    }
    else if (wp0 > 0 && wp1 > 0)
    {
        if (sb0.seq_no >= sb1.seq_no)
        {
            t->durable_sb = sb0;
            t->active_zone = META_ZONE_0;
            t->meta_wp = wp0 + 1;
            t->version = zh0.version;
        }
        else
        {
            t->durable_sb = sb1;
            t->active_zone = META_ZONE_1;
            t->meta_wp = wp1 + 1;
            t->version = zh1.version;
        }
    }
    else if (wp0 > 0)
    {
        t->durable_sb = sb0;
        t->active_zone = META_ZONE_0;
        t->meta_wp = wp0 + 1;
        t->version = zh0.version;
    }
    else
    {
        t->durable_sb = sb1;
        t->active_zone = META_ZONE_1;
        t->meta_wp = wp1 + 1;
        t->version = zh1.version;
    }

    atomic_store_explicit(&t->volatile_sb.root_pn, t->durable_sb.root_pn, memory_order_release);
    atomic_store_explicit(&t->volatile_sb.seq_no, t->durable_sb.seq_no * 2, memory_order_release);
}

static void write_superblock_sync(cow_tree *t)
{
    pthread_mutex_lock(&t->flush_lock);

    t->durable_sb.magic = SB_MAGIC;

    if (t->meta_wp >= t->info.zone_size / PAGE_SIZE)
    {
        uint32_t new_zone = 1 - t->active_zone;
        activate_meta_zone(t, new_zone, t->version + 1);
    }

    pagenum_t ignored_pn;
    uint64_t wp_bytes;
    if (zone_append_raw_nolock(t, t->active_zone, &t->durable_sb, &ignored_pn, &wp_bytes) != 0)
    {
        pthread_mutex_unlock(&t->flush_lock);
        perror("nvme_zns_append(superblock)");
        exit(EXIT_FAILURE);
    }

    atomic_store_explicit(&t->zone_wp_bytes[t->active_zone], wp_bytes, memory_order_release);
    atomic_store_explicit(
        &t->zone_full[t->active_zone],
        (wp_bytes >= t->zones[t->active_zone].start + t->zones[t->active_zone].capacity) ? 1 : 0,
        memory_order_release);

    t->meta_wp++;
    pthread_mutex_unlock(&t->flush_lock);
}

static void *sb_flusher_thread(void *arg)
{
    cow_tree *t = (cow_tree *)arg;

    while (!atomic_load_explicit(&t->flusher_stop, memory_order_acquire))
    {
        usleep(FLUSH_INTERVAL_MS * 1000);

        if (!atomic_exchange_explicit(&t->dirty, false, memory_order_acq_rel))
        {
            continue;
        }

        pagenum_t root;
        uint64_t seq;
        read_tree_snapshot(t, &root, &seq);

        pthread_mutex_lock(&t->flush_lock);
        t->durable_sb.root_pn = root;
        t->durable_sb.seq_no = seq / 2;
        pthread_mutex_unlock(&t->flush_lock);

        write_superblock_sync(t);
    }

    return NULL;
}

static void publish_root_single_writer(cow_tree *t, pagenum_t new_root)
{
    uint64_t s = atomic_load_explicit(&t->volatile_sb.seq_no, memory_order_acquire);
    if (s & 1ULL)
        s++;

    atomic_store_explicit(&t->volatile_sb.seq_no, s + 1, memory_order_release);
    atomic_store_explicit(&t->volatile_sb.root_pn, new_root, memory_order_release);
    atomic_store_explicit(&t->volatile_sb.seq_no, s + 2, memory_order_release);
    atomic_store_explicit(&t->dirty, true, memory_order_release);
}

static inline uint64_t overlay_hash_u64(uint64_t x)
{
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

static void overlay_index_grow(overlay_state *ov)
{
    size_t new_cap = ov->idx_cap ? (ov->idx_cap << 1) : 512;
    size_t *new_tab = malloc(new_cap * sizeof(*new_tab));
    if (!new_tab)
    {
        perror("malloc overlay idx_table");
        exit(EXIT_FAILURE);
    }
    for (size_t i = 0; i < new_cap; i++)
    {
        new_tab[i] = (size_t)-1;
    }

    for (size_t i = 0; i < ov->len; i++)
    {
        node_id_t id = ov->arr[i].id;
        size_t m = new_cap - 1;
        size_t pos = (size_t)(overlay_hash_u64((uint64_t)id) & m);
        while (new_tab[pos] != (size_t)-1)
        {
            pos = (pos + 1) & m;
        }
        new_tab[pos] = i;
    }

    free(ov->idx_table);
    ov->idx_table = new_tab;
    ov->idx_cap = new_cap;
    ov->idx_used = ov->len;
}

static int overlay_find_idx(overlay_state *ov, node_id_t id)
{
    if (ov->idx_cap == 0)
    {
        return -1;
    }

    size_t m = ov->idx_cap - 1;
    size_t pos = (size_t)(overlay_hash_u64((uint64_t)id) & m);
    for (;;)
    {
        size_t v = ov->idx_table[pos];
        if (v == (size_t)-1)
        {
            return -1;
        }
        if (ov->arr[v].id == id)
        {
            return (int)v;
        }
        pos = (pos + 1) & m;
    }
}

static void overlay_index_insert(overlay_state *ov, node_id_t id, size_t idx)
{
    if (ov->idx_cap == 0 || (ov->idx_used + 1) * 10 >= ov->idx_cap * 7)
    {
        overlay_index_grow(ov);
    }

    size_t m = ov->idx_cap - 1;
    size_t pos = (size_t)(overlay_hash_u64((uint64_t)id) & m);
    while (ov->idx_table[pos] != (size_t)-1)
    {
        pos = (pos + 1) & m;
    }
    ov->idx_table[pos] = idx;
    ov->idx_used++;
}

static overlay_node *overlay_add_node(overlay_state *ov, node_id_t id, const page *src)
{
    if (ov->len == ov->cap)
    {
        size_t new_cap = ov->cap ? ov->cap * 2 : 256;
        overlay_node *next = realloc(ov->arr, new_cap * sizeof(*next));
        if (!next)
        {
            perror("realloc overlay");
            exit(EXIT_FAILURE);
        }
        ov->arr = next;
        ov->cap = new_cap;
    }

    size_t idx = ov->len;
    overlay_node *n = &ov->arr[ov->len++];
    n->id = id;
    if (src)
        n->node = *src;
    else
        memset(&n->node, 0, sizeof(n->node));
    n->dirty = 0;
    n->flushed = 0;
    n->flushed_pn = INVALID_PGN;

    overlay_index_insert(ov, id, idx);
    return n;
}

static overlay_node *overlay_get_mut(overlay_state *ov, node_id_t id)
{
    int idx = overlay_find_idx(ov, id);
    if (idx >= 0)
        return &ov->arr[idx];

    if (is_temp_id(id))
    {
        return NULL;
    }

    page p;
    load_page(ov->t, (pagenum_t)id, &p);
    return overlay_add_node(ov, id, &p);
}

static node_id_t overlay_new_temp(overlay_state *ov, uint32_t is_leaf)
{
    node_id_t id = make_temp_id(++ov->next_temp);
    overlay_node *n = overlay_add_node(ov, id, NULL);
    n->node.is_leaf = is_leaf;
    n->dirty = 1;
    return id;
}

static uint32_t get_position(page *p, int64_t key)
{
    if (p->is_leaf)
    {
        for (uint32_t i = 0; i < p->num_keys; i++)
        {
            if (key < (int64_t)p->leaf[i].key)
                return i;
        }
    }
    else
    {
        for (uint32_t i = 0; i < p->num_keys; i++)
        {
            if (key < (int64_t)p->internal[i].key)
                return i;
        }
    }
    return p->num_keys;
}

static void apply_insert_overlay(overlay_state *ov, node_id_t *root_id, int64_t key, const char *value)
{
    if (*root_id == INVALID_PGN)
    {
        node_id_t rid = overlay_new_temp(ov, 1);
        overlay_node *r = overlay_get_mut(ov, rid);
        r->node.num_keys = 1;
        r->node.pointer = INVALID_PGN;
        r->node.leaf[0].key = (uint64_t)key;
        memcpy(r->node.leaf[0].record.value, value, 120);
        r->dirty = 1;
        *root_id = rid;
        return;
    }

    overlay_path path;
    path.depth = 0;

    node_id_t cur = *root_id;
    while (1)
    {
        overlay_node *n = overlay_get_mut(ov, cur);
        if (!n)
        {
            fprintf(stderr, "overlay: missing node %lu\n", (unsigned long)cur);
            exit(EXIT_FAILURE);
        }

        if (n->node.is_leaf)
        {
            break;
        }

        if (path.depth >= MAX_HEIGHT)
        {
            fprintf(stderr, "tree depth exceeded MAX_HEIGHT\n");
            exit(EXIT_FAILURE);
        }

        uint32_t idx = RIGHTMOST_IDX;
        for (uint32_t i = 0; i < n->node.num_keys; i++)
        {
            if (key < (int64_t)n->node.internal[i].key)
            {
                idx = i;
                break;
            }
        }

        path.e[path.depth].id = cur;
        path.e[path.depth].cidx = idx;
        path.depth++;

        cur = (idx == RIGHTMOST_IDX) ? n->node.pointer : n->node.internal[idx].child;
    }

    overlay_node *leaf_n = overlay_get_mut(ov, cur);
    page *leaf = &leaf_n->node;

    for (uint32_t i = 0; i < leaf->num_keys; i++)
    {
        if ((int64_t)leaf->leaf[i].key == key)
        {
            memcpy(leaf->leaf[i].record.value, value, 120);
            leaf_n->dirty = 1;
            return;
        }
    }

    node_id_t carry_left = cur;
    node_id_t carry_right = INVALID_PGN;
    int carry_split = 0;
    int64_t carry_key = 0;

    if (leaf->num_keys < LEAF_ORDER - 1)
    {
        uint32_t pos = get_position(leaf, key);
        for (int64_t i = (int64_t)leaf->num_keys - 1; i >= (int64_t)pos; i--)
        {
            leaf->leaf[i + 1] = leaf->leaf[i];
        }
        leaf->leaf[pos].key = (uint64_t)key;
        memcpy(leaf->leaf[pos].record.value, value, 120);
        leaf->num_keys++;
        leaf_n->dirty = 1;
        carry_split = 0;
    }
    else
    {
        leaf_entity tmp[LEAF_ORDER];
        uint32_t pos = 0;
        while (pos < leaf->num_keys && (int64_t)leaf->leaf[pos].key < key)
            pos++;

        for (uint32_t i = 0; i < pos; i++)
            tmp[i] = leaf->leaf[i];
        for (uint32_t i = pos; i < leaf->num_keys; i++)
            tmp[i + 1] = leaf->leaf[i];

        tmp[pos].key = (uint64_t)key;
        memcpy(tmp[pos].record.value, value, 120);

        uint32_t sp = LEAF_ORDER / 2;
        for (uint32_t i = 0; i < sp; i++)
            leaf->leaf[i] = tmp[i];
        leaf->num_keys = sp;

        node_id_t right_id = overlay_new_temp(ov, 1);
        overlay_node *right_n = overlay_get_mut(ov, right_id);
        right_n->node.num_keys = LEAF_ORDER - sp;
        for (uint32_t i = 0; i < LEAF_ORDER - sp; i++)
        {
            right_n->node.leaf[i] = tmp[sp + i];
        }
        right_n->node.pointer = leaf->pointer;

        leaf->pointer = right_id;
        leaf_n->dirty = 1;
        right_n->dirty = 1;

        carry_split = 1;
        carry_right = right_id;
        carry_key = (int64_t)right_n->node.leaf[0].key;
    }

    while (path.depth > 0)
    {
        overlay_path_entry pe = path.e[--path.depth];
        overlay_node *par_n = overlay_get_mut(ov, pe.id);
        page *par = &par_n->node;

        uint32_t cidx = pe.cidx;
        uint32_t pos = (cidx == RIGHTMOST_IDX) ? par->num_keys : cidx;

        if (cidx == RIGHTMOST_IDX)
            par->pointer = carry_left;
        else
            par->internal[cidx].child = carry_left;

        if (!carry_split)
        {
            par_n->dirty = 1;
            carry_left = pe.id;
            continue;
        }

        if (par->num_keys < INTERNAL_ORDER - 1)
        {
            for (int64_t j = (int64_t)par->num_keys - 1; j >= (int64_t)pos; j--)
            {
                par->internal[j + 1] = par->internal[j];
            }

            par->internal[pos].key = (uint64_t)carry_key;
            par->internal[pos].child = carry_left;

            if (pos == par->num_keys)
                par->pointer = carry_right;
            else
                par->internal[pos + 1].child = carry_right;

            par->num_keys++;
            par_n->dirty = 1;

            carry_split = 0;
            carry_left = pe.id;
            continue;
        }

        int64_t tkeys[INTERNAL_ORDER];
        node_id_t tchld[INTERNAL_ORDER + 1];

        for (uint32_t j = 0; j < pos; j++)
            tkeys[j] = (int64_t)par->internal[j].key;
        tkeys[pos] = carry_key;
        for (uint32_t j = pos; j < INTERNAL_ORDER - 1; j++)
            tkeys[j + 1] = (int64_t)par->internal[j].key;

        for (uint32_t j = 0; j < pos; j++)
            tchld[j] = par->internal[j].child;
        tchld[pos] = carry_left;
        tchld[pos + 1] = carry_right;
        for (uint32_t j = pos + 1; j < INTERNAL_ORDER; j++)
        {
            tchld[j + 1] = (j < INTERNAL_ORDER - 1) ? par->internal[j].child : par->pointer;
        }

        uint32_t sp = (INTERNAL_ORDER + 1) / 2;
        int64_t up_key = tkeys[sp - 1];

        for (uint32_t j = 0; j < sp - 1; j++)
        {
            par->internal[j].key = (uint64_t)tkeys[j];
            par->internal[j].child = tchld[j];
        }
        par->pointer = tchld[sp - 1];
        par->num_keys = sp - 1;
        par_n->dirty = 1;

        node_id_t right_id = overlay_new_temp(ov, 0);
        overlay_node *right_n = overlay_get_mut(ov, right_id);
        for (uint32_t j = sp; j < INTERNAL_ORDER; j++)
        {
            right_n->node.internal[j - sp].key = (uint64_t)tkeys[j];
            right_n->node.internal[j - sp].child = tchld[j];
        }
        right_n->node.pointer = tchld[INTERNAL_ORDER];
        right_n->node.num_keys = INTERNAL_ORDER - sp;
        right_n->dirty = 1;

        carry_split = 1;
        carry_left = pe.id;
        carry_right = right_id;
        carry_key = up_key;
    }

    if (carry_split)
    {
        node_id_t new_root_id = overlay_new_temp(ov, 0);
        overlay_node *r = overlay_get_mut(ov, new_root_id);
        r->node.num_keys = 1;
        r->node.internal[0].key = (uint64_t)carry_key;
        r->node.internal[0].child = carry_left;
        r->node.pointer = carry_right;
        r->dirty = 1;
        *root_id = new_root_id;
    }
}


/*
 * Resolve a child node_id to its final on-disk pagenum.
 * Called after Phase 1 collection, when flushed_pn is already set for all
 * nodes in the overlay (either to the real on-disk id, or to base_pn+idx).
 */
static pagenum_t resolve_node_pn(overlay_state *ov, node_id_t child_id)
{
    if (child_id == INVALID_PGN)
        return INVALID_PGN;
    int idx = overlay_find_idx(ov, child_id);
    if (idx >= 0)
        return ov->arr[idx].flushed_pn;
    return (pagenum_t)child_id;
}

/*
 * Phase 1: post-order DFS collecting every overlay node that needs writing.
 *
 * A node needs writing if it is dirty/temp, or any of its children got a
 * new pagenum (COW propagation).  Children are always processed before their
 * parent, guaranteeing that when we build a parent page its children's
 * flushed_pn values are already final.
 *
 * Nodes that do not need rewriting are marked flushed with flushed_pn == id
 * (their existing on-disk pagenum).  Nodes that do need rewriting are added
 * to entries[] and marked flushed with flushed_pn == batch_index (temporary).
 *
 * Returns 1 if this subtree will produce a new pagenum for id.
 */
static int collect_dirty_nodes(overlay_state *ov, node_id_t id,
                                batch_entry *entries, int *count, int max)
{
    if (id == INVALID_PGN)
        return 0;

    int idx = overlay_find_idx(ov, id);
    if (idx < 0)
        return 0; /* not in overlay — on-disk node, id is already the pagenum */

    overlay_node *n = &ov->arr[idx];
    if (n->flushed)
        return (n->flushed_pn != (pagenum_t)n->id || is_temp_id(n->id));

    int child_changed = 0;

    if (!n->node.is_leaf)
    {
        for (uint32_t i = 0; i < n->node.num_keys; i++)
            child_changed |= collect_dirty_nodes(ov, n->node.internal[i].child,
                                                  entries, count, max);
        child_changed |= collect_dirty_nodes(ov, n->node.pointer, entries, count, max);
    }

    if (n->dirty || child_changed || is_temp_id(n->id))
    {
        n->flushed = 1;
        n->flushed_pn = (pagenum_t)(*count); /* temp: will become base_pn + idx */
        if (*count < max)
        {
            entries[*count].n = n;
            (*count)++;
        }
        return 1;
    }
    else
    {
        n->flushed = 1;
        n->flushed_pn = (pagenum_t)n->id; /* unchanged on-disk node */
        return 0;
    }
}

/*
 * Flush all dirty overlay nodes as a single (chunked) NVMe ZNS append.
 *
 * Algorithm:
 *   1. Post-order DFS to collect dirty nodes; flushed_pn = batch index.
 *   2. Pick a zone that has enough space for count pages; predict base_pn
 *      from the software write-pointer (safe because commit_exec_lock gives
 *      us exclusive write access, so the sw-WP matches the hw-WP exactly).
 *   3. Assign real pagenums: flushed_pn = base_pn + index.
 *   4. Build page data (internal nodes resolve child pointers via flushed_pn).
 *   5. Append all pages in MAX_NVME_PAGES-sized NVMe commands.
 *   6. Insert every page into the RAM cache.
 */
static pagenum_t flush_overlay_batched(overlay_state *ov, node_id_t root_id)
{
    if (root_id == INVALID_PGN)
        return INVALID_PGN;

    cow_tree *t = ov->t;
    batch_entry entries[MAX_BATCH_PAGES];
    int count = 0;

    collect_dirty_nodes(ov, root_id, entries, &count, MAX_BATCH_PAGES);

    /* Determine root's pagenum (may still be temp batch index at this point) */
    int root_idx = overlay_find_idx(ov, root_id);

    if (count == 0)
    {
        /* Nothing to write; root is an unchanged on-disk node */
        return (root_idx >= 0) ? ov->arr[root_idx].flushed_pn : (pagenum_t)root_id;
    }

    /* ------------------------------------------------------------------ */
    /* Zone selection: find a zone with room for count pages               */
    /* ------------------------------------------------------------------ */
    uint32_t zone_id;
    uint64_t base_wp;

    for (;;)
    {
        zone_id = atomic_load_explicit(&t->current_zone, memory_order_acquire);
        if (zone_id < DATA_ZONE_START)
        {
            uint32_t expected = zone_id;
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, (uint32_t)DATA_ZONE_START,
                memory_order_acq_rel, memory_order_acquire);
            zone_id = DATA_ZONE_START;
        }
        if (zone_id >= t->info.nr_zones)
        {
            fprintf(stderr, "zones exhausted\n");
            exit(EXIT_FAILURE);
        }
        if (atomic_load_explicit(&t->zone_full[zone_id], memory_order_acquire))
        {
            uint32_t next = zone_next(t, zone_id);
            uint32_t expected = zone_id;
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, next,
                memory_order_acq_rel, memory_order_acquire);
            continue;
        }

        base_wp = atomic_load_explicit(&t->zone_wp_bytes[zone_id], memory_order_acquire);
        uint64_t zone_end = t->zones[zone_id].start + t->zones[zone_id].capacity;
        uint64_t needed   = (uint64_t)count * PAGE_SIZE;

        if (base_wp + needed > zone_end)
        {
            /* Doesn't fit — mark full and rotate */
            atomic_store_explicit(&t->zone_full[zone_id], 1, memory_order_release);
            uint32_t next = zone_next(t, zone_id);
            uint32_t expected = zone_id;
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, next,
                memory_order_acq_rel, memory_order_acquire);
            atomic_fetch_add_explicit(&t->stat_zone_rotations, 1, memory_order_relaxed);
            continue;
        }
        break;
    }

    /*
     * Predict base_pn from the software write-pointer.
     * Under commit_exec_lock we are the sole writer, so sw-WP == hw-WP.
     */
    pagenum_t base_pn = (pagenum_t)(base_wp / PAGE_SIZE);

    /* ------------------------------------------------------------------ */
    /* Assign real pagenums to every entry                                 */
    /* ------------------------------------------------------------------ */
    for (int i = 0; i < count; i++)
        entries[i].n->flushed_pn = base_pn + (pagenum_t)i;

    /* ------------------------------------------------------------------ */
    /* Build page data — child pointers are now resolvable                 */
    /* ------------------------------------------------------------------ */
    void *buf;
    if (posix_memalign(&buf, PAGE_SIZE, (size_t)count * PAGE_SIZE) != 0)
    {
        perror("posix_memalign batch flush");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < count; i++)
    {
        overlay_node *n = entries[i].n;
        page out = n->node;
        out.pn = base_pn + (pagenum_t)i;

        if (!out.is_leaf)
        {
            for (uint32_t j = 0; j < out.num_keys; j++)
                out.internal[j].child = resolve_node_pn(ov, out.internal[j].child);
            out.pointer = resolve_node_pn(ov, out.pointer);
        }

        memcpy((char *)buf + (size_t)i * PAGE_SIZE, &out, sizeof(page));
    }

    /* ------------------------------------------------------------------ */
    /* Append to NVMe in MAX_NVME_PAGES-sized chunks                       */
    /* ------------------------------------------------------------------ */
    int done = 0;
    pagenum_t actual_base = 0;

    while (done < count)
    {
        int chunk = count - done;
        if (chunk > MAX_NVME_PAGES)
            chunk = MAX_NVME_PAGES;

        char    *chunk_buf   = (char *)buf + (size_t)done * PAGE_SIZE;
        uint32_t total_bytes = (uint32_t)chunk * PAGE_SIZE;
        __u16    nlb         = (__u16)((total_bytes / t->info.lblock_size) - 1);
        __u64    zslba       = t->zones[zone_id].start / t->info.lblock_size;
        __u64    result      = 0;

        struct nvme_zns_append_args args = {
            .zslba        = zslba,
            .result       = &result,
            .data         = chunk_buf,
            .metadata     = NULL,
            .args_size    = sizeof(args),
            .fd           = t->fd,
            .timeout      = 0,
            .nsid         = t->nsid,
            .ilbrt        = 0,
            .data_len     = total_bytes,
            .metadata_len = 0,
            .nlb          = nlb,
            .control      = 0,
            .lbat         = 0,
            .lbatm        = 0,
            .ilbrt_u64    = 0,
        };

        if (nvme_zns_append(&args) != 0)
        {
            perror("nvme_zns_append (batch)");
            exit(EXIT_FAILURE);
        }

        pagenum_t chunk_base = (pagenum_t)(result * t->info.lblock_size / PAGE_SIZE);
        if (done == 0)
            actual_base = chunk_base;

        /* Update zone WP from hardware result */
        uint64_t new_wp  = (result + (__u64)nlb + 1) * t->info.lblock_size;
        uint64_t cur_wp  = atomic_load_explicit(&t->zone_wp_bytes[zone_id], memory_order_acquire);
        while (new_wp > cur_wp &&
               !atomic_compare_exchange_weak_explicit(
                   &t->zone_wp_bytes[zone_id], &cur_wp, new_wp,
                   memory_order_acq_rel, memory_order_acquire))
        {
        }

        done += chunk;
    }

    /*
     * If the hardware placed pages at a different base than predicted
     * (should not happen under commit_exec_lock, but handle defensively),
     * correct flushed_pn and page.pn for every entry.
     */
    if (actual_base != base_pn)
    {
        pagenum_t delta = actual_base - base_pn;
        for (int i = 0; i < count; i++)
        {
            entries[i].n->flushed_pn += delta;
            page *pg = (page *)((char *)buf + (size_t)i * PAGE_SIZE);
            pg->pn  += delta;
        }
    }

    /* ------------------------------------------------------------------ */
    /* Insert all pages into the RAM table                                 */
    /* ------------------------------------------------------------------ */
    for (int i = 0; i < count; i++)
    {
        overlay_node *n  = entries[i].n;
        page         *pg = (page *)((char *)buf + (size_t)i * PAGE_SIZE);
        ram_table_insert(t, n->flushed_pn, pg);
    }

    atomic_fetch_add_explicit(&t->stat_page_appends, (uint64_t)count, memory_order_relaxed);

    /* Check / update zone-full state after the append */
    {
        uint64_t cur_wp  = atomic_load_explicit(&t->zone_wp_bytes[zone_id], memory_order_acquire);
        uint64_t zone_end = t->zones[zone_id].start + t->zones[zone_id].capacity;
        if (cur_wp >= zone_end)
        {
            atomic_store_explicit(&t->zone_full[zone_id], 1, memory_order_release);
            uint32_t next     = zone_next(t, zone_id);
            uint32_t expected = zone_id;
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, next,
                memory_order_acq_rel, memory_order_acquire);
        }
    }

    free(buf);

    return (root_idx >= 0) ? ov->arr[root_idx].flushed_pn : (pagenum_t)root_id;
}

static void stage2_enqueue_job(cow_tree *t, txg_batch_job *job)
{
    pthread_mutex_lock(&t->stage2_lock);

    if (t->stage2_tail)
        t->stage2_tail->next = job;
    else
        t->stage2_head = job;

    t->stage2_tail = job;
    pthread_cond_signal(&t->stage2_cv);
    pthread_mutex_unlock(&t->stage2_lock);
}

static txg_batch_job *stage2_dequeue_job(cow_tree *t)
{
    pthread_mutex_lock(&t->stage2_lock);

    while (t->stage2_head == NULL &&
           !atomic_load_explicit(&t->stop_commit, memory_order_acquire))
    {
        pthread_cond_wait(&t->stage2_cv, &t->stage2_lock);
    }

    txg_batch_job *job = t->stage2_head;
    if (job)
    {
        t->stage2_head = job->next;
        if (t->stage2_head == NULL)
            t->stage2_tail = NULL;
    }

    pthread_mutex_unlock(&t->stage2_lock);
    return job;
}

static int pop_batch(cow_tree *t, insert_req **batch, int max_batch)
{
    int n = 0;

    uint64_t qwait_start = monotonic_ns();
    pthread_mutex_lock(&t->q_lock);
    uint64_t qwait_end = monotonic_ns();
    atomic_fetch_add_explicit(&t->stat_q_lock_wait_ns_sync, qwait_end - qwait_start, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_q_lock_wait_samples_sync, 1, memory_order_relaxed);

    while (!atomic_load_explicit(&t->stop_sync, memory_order_acquire) && t->q_head == NULL)
    {
        atomic_fetch_add_explicit(&t->stat_sync_idle_waits, 1, memory_order_relaxed);
        pthread_cond_wait(&t->q_cv, &t->q_lock);
    }

    if (atomic_load_explicit(&t->stop_sync, memory_order_acquire) && t->q_head == NULL)
    {
        pthread_mutex_unlock(&t->q_lock);
        return 0;
    }

    while (n < max_batch && t->q_head != NULL)
    {
        insert_req *req = t->q_head;
        t->q_head = req->next;
        if (t->q_head == NULL)
            t->q_tail = NULL;
        req->next = NULL;
        batch[n++] = req;
    }

    int drained = n;
    if (drained > 0)
    {
        uint64_t qcur = atomic_fetch_sub_explicit(&t->stat_queue_depth_current, (uint64_t)drained, memory_order_relaxed) - (uint64_t)drained;
        atomic_fetch_add_explicit(&t->stat_queue_depth_sum, qcur, memory_order_relaxed);
        atomic_fetch_add_explicit(&t->stat_queue_depth_samples, 1, memory_order_relaxed);
    }

    if (n < max_batch && !atomic_load_explicit(&t->stop_sync, memory_order_acquire))
    {
        uint64_t qdepth_cur = atomic_load_explicit(&t->stat_queue_depth_current, memory_order_relaxed);
        if (n >= 4 || qdepth_cur >= 8)
        {
            uint32_t wait_us = (n >= 16 || qdepth_cur >= 32) ? TXG_BATCH_WAIT_US : TXG_BATCH_MIN_WAIT_US;

            struct timespec now;
            clock_gettime(CLOCK_REALTIME, &now);
            uint64_t deadline_ns = (uint64_t)now.tv_nsec + (uint64_t)wait_us * 1000ULL;
            struct timespec deadline = {
                .tv_sec = now.tv_sec + (time_t)(deadline_ns / 1000000000ULL),
                .tv_nsec = (long)(deadline_ns % 1000000000ULL)};

            for (;;)
            {
                if (n >= max_batch || atomic_load_explicit(&t->stop_sync, memory_order_acquire))
                    break;

                int rc = pthread_cond_timedwait(&t->q_cv, &t->q_lock, &deadline);
                if (rc == ETIMEDOUT)
                    break;
                if (rc != 0)
                    break;

                while (n < max_batch && t->q_head != NULL)
                {
                    insert_req *req = t->q_head;
                    t->q_head = req->next;
                    if (t->q_head == NULL)
                        t->q_tail = NULL;
                    req->next = NULL;
                    batch[n++] = req;
                }
            }
        }
    }

    int extra = n - drained;
    if (extra > 0)
    {
        uint64_t qcur = atomic_fetch_sub_explicit(&t->stat_queue_depth_current, (uint64_t)extra, memory_order_relaxed) - (uint64_t)extra;
        atomic_fetch_add_explicit(&t->stat_queue_depth_sum, qcur, memory_order_relaxed);
        atomic_fetch_add_explicit(&t->stat_queue_depth_samples, 1, memory_order_relaxed);
    }

    pthread_mutex_unlock(&t->q_lock);
    return n;
}

static void complete_req(insert_req *req)
{
    pthread_mutex_lock(&req->done_lock);
    req->done = 1;
    pthread_cond_signal(&req->done_cv);
    pthread_mutex_unlock(&req->done_lock);
}

static int cmp_batch_item(const void *a, const void *b)
{
    const batch_item *x = (const batch_item *)a;
    const batch_item *y = (const batch_item *)b;

    if (x->req->key < y->req->key)
        return -1;
    if (x->req->key > y->req->key)
        return 1;

    if (x->ord < y->ord)
        return -1;
    if (x->ord > y->ord)
        return 1;
    return 0;
}

static void process_txg_jobs(cow_tree *t, txg_batch_job **jobs, int njobs, int free_jobs)
{
    batch_item merged_items[TXG_COMMIT_MERGE_ITEMS];
    int merged_n = 0;
    uint64_t start_ns = jobs[0]->start_ns;

    for (int j = 0; j < njobs; j++)
    {
        txg_batch_job *cur = jobs[j];
        if (cur->start_ns < start_ns)
            start_ns = cur->start_ns;

        for (int i = 0; i < cur->n; i++)
        {
            if (merged_n >= TXG_COMMIT_MERGE_ITEMS)
                break;
            merged_items[merged_n] = cur->items[i];
            merged_n++;
        }
    }

    if (merged_n <= 0)
    {
        if (free_jobs)
        {
            for (int j = 0; j < njobs; j++)
                free(jobs[j]);
        }
        return;
    }

    // Only sort when there are at least two items.
    if (merged_n > 1)
        qsort(merged_items, (size_t)merged_n, sizeof(merged_items[0]), cmp_batch_item);

    pthread_mutex_lock(&t->commit_exec_lock);

    pagenum_t root;
    uint64_t seq;
    read_tree_snapshot(t, &root, &seq);
    (void)seq;

    overlay_state ov;
    memset(&ov, 0, sizeof(ov));
    ov.t = t;

    node_id_t root_id = root;
    uint64_t apply_t0 = monotonic_ns();
    for (int i = 0; i < merged_n; i++)
        apply_insert_overlay(&ov, &root_id, merged_items[i].req->key, merged_items[i].req->value);
    uint64_t apply_dt = monotonic_ns() - apply_t0;
    atomic_fetch_add_explicit(&t->stat_apply_latency_ns_sum, apply_dt, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_apply_latency_ns_samples, 1, memory_order_relaxed);

    uint64_t flush_t0 = monotonic_ns();
    pagenum_t new_root = flush_overlay_batched(&ov, root_id);
    uint64_t flush_dt = monotonic_ns() - flush_t0;
    atomic_fetch_add_explicit(&t->stat_flush_latency_ns_sum, flush_dt, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_flush_latency_ns_samples, 1, memory_order_relaxed);

    atomic_fetch_add_explicit(&t->stat_overlay_nodes_sum, (uint64_t)ov.len, memory_order_relaxed);
    stat_update_max_u64(&t->stat_overlay_nodes_max, (uint64_t)ov.len);
    free(ov.arr);
    free(ov.idx_table);

    publish_root_single_writer(t, new_root);

    uint64_t txg_dt = monotonic_ns() - start_ns;
    atomic_fetch_add_explicit(&t->stat_batch_latency_ns_sum, txg_dt, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_batch_latency_ns_samples, 1, memory_order_relaxed);

    pthread_mutex_unlock(&t->commit_exec_lock);

    for (int i = 0; i < merged_n; i++)
        complete_req(merged_items[i].req);

    if (free_jobs)
    {
        for (int j = 0; j < njobs; j++)
            free(jobs[j]);
    }
}

static void *txg_batch_main(void *arg)
{
    cow_tree *t = (cow_tree *)arg;
    insert_req *batch[TXG_BATCH_MAX];

    for (;;)
    {
        uint64_t txg_t0 = monotonic_ns();
        int n = pop_batch(t, batch, TXG_BATCH_MAX);
        if (n == 0)
            break;

        int inline_fast = 0;
        if (n <= 2)
        {
            uint64_t qdepth_cur = atomic_load_explicit(&t->stat_queue_depth_current, memory_order_relaxed);
            int stage2_empty;
            pthread_mutex_lock(&t->stage2_lock);
            stage2_empty = (t->stage2_head == NULL);
            pthread_mutex_unlock(&t->stage2_lock);
            inline_fast = (stage2_empty && qdepth_cur <= 2);
        }

        txg_batch_job inline_job;
        txg_batch_job *job = &inline_job;
        if (!inline_fast)
        {
            job = malloc(sizeof(*job));
            if (!job)
            {
                perror("malloc txg_batch_job");
                exit(EXIT_FAILURE);
            }
        }

        job->start_ns = txg_t0;
        job->n = n;
        job->next = NULL;

        atomic_fetch_add_explicit(&t->stat_batches, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&t->stat_batch_items, (uint64_t)n, memory_order_relaxed);

        for (int i = 0; i < n; i++)
        {
            job->items[i].req = batch[i];
            job->items[i].ord = i;
        }

        uint64_t sort_t0 = monotonic_ns();
        if (n > 1)
            qsort(job->items, (size_t)n, sizeof(job->items[0]), cmp_batch_item);
        uint64_t sort_dt = monotonic_ns() - sort_t0;
        atomic_fetch_add_explicit(&t->stat_sort_latency_ns_sum, sort_dt, memory_order_relaxed);
        atomic_fetch_add_explicit(&t->stat_sort_latency_ns_samples, 1, memory_order_relaxed);

        if (inline_fast)
        {
            txg_batch_job *single[1] = {job};
            process_txg_jobs(t, single, 1, 0);
            continue;
        }

        stage2_enqueue_job(t, job);
    }

    atomic_store_explicit(&t->stop_commit, true, memory_order_release);
    pthread_mutex_lock(&t->stage2_lock);
    pthread_cond_broadcast(&t->stage2_cv);
    pthread_mutex_unlock(&t->stage2_lock);

    return NULL;
}

static void *txg_commit_main(void *arg)
{
    cow_tree *t = (cow_tree *)arg;
    txg_batch_job *jobs[TXG_COMMIT_MERGE_JOBS];

    for (;;)
    {
        txg_batch_job *job = stage2_dequeue_job(t);
        if (!job)
        {
            if (atomic_load_explicit(&t->stop_commit, memory_order_acquire))
                break;
            continue;
        }

        int njobs = 1;
        int planned_items = job->n;
        jobs[0] = job;

        for (;;)
        {
            if (njobs >= TXG_COMMIT_MERGE_JOBS)
                break;

            pthread_mutex_lock(&t->stage2_lock);
            txg_batch_job *next = t->stage2_head;
            if (!next || (planned_items + next->n) > TXG_COMMIT_MERGE_ITEMS)
            {
                pthread_mutex_unlock(&t->stage2_lock);
                break;
            }

            t->stage2_head = next->next;
            if (t->stage2_head == NULL)
                t->stage2_tail = NULL;
            pthread_mutex_unlock(&t->stage2_lock);

            jobs[njobs++] = next;
            planned_items += next->n;
        }

        process_txg_jobs(t, jobs, njobs, 1);
    }

    return NULL;
}

cow_tree *cow_open(const char *path)
{
    cow_tree *t = malloc(sizeof *t);
    if (!t)
    {
        perror("malloc");
        return NULL;
    }
    memset(t, 0, sizeof *t);

    if (pthread_mutex_init(&t->flush_lock, NULL) != 0)
    {
        perror("pthread_mutex_init flush_lock");
        free(t);
        return NULL;
    }
    if (pthread_mutex_init(&t->q_lock, NULL) != 0)
    {
        perror("pthread_mutex_init q_lock");
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }
    if (pthread_cond_init(&t->q_cv, NULL) != 0)
    {
        perror("pthread_cond_init q_cv");
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }
    if (pthread_mutex_init(&t->stage2_lock, NULL) != 0)
    {
        perror("pthread_mutex_init stage2_lock");
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }
    if (pthread_cond_init(&t->stage2_cv, NULL) != 0)
    {
        perror("pthread_cond_init stage2_cv");
        pthread_mutex_destroy(&t->stage2_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }
    if (pthread_mutex_init(&t->commit_exec_lock, NULL) != 0)
    {
        perror("pthread_mutex_init commit_exec_lock");
        pthread_cond_destroy(&t->stage2_cv);
        pthread_mutex_destroy(&t->stage2_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }

    t->fd = zbd_open(path, O_RDWR, &t->info);
    if (t->fd < 0)
    {
        perror("zbd_open");
        pthread_cond_destroy(&t->stage2_cv);
        pthread_mutex_destroy(&t->stage2_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        pthread_mutex_destroy(&t->commit_exec_lock);
        free(t);
        return NULL;
    }

    if (nvme_get_nsid(t->fd, &t->nsid) != 0)
    {
        perror("nvme_get_nsid");
        zbd_close(t->fd);
        pthread_cond_destroy(&t->stage2_cv);
        pthread_mutex_destroy(&t->stage2_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        pthread_mutex_destroy(&t->commit_exec_lock);
        free(t);
        return NULL;
    }

    t->direct_fd = open(path, O_RDONLY | O_DIRECT);

    t->zones = calloc(t->info.nr_zones, sizeof *t->zones);
    t->zone_wp_bytes = calloc(t->info.nr_zones, sizeof(*t->zone_wp_bytes));
    t->zone_full = calloc(t->info.nr_zones, sizeof(*t->zone_full));
    if (!t->zones || !t->zone_wp_bytes || !t->zone_full)
    {
        perror("calloc");
        free(t->zones);
        free(t->zone_wp_bytes);
        free(t->zone_full);
        if (t->direct_fd >= 0)
            close(t->direct_fd);
        zbd_close(t->fd);
        pthread_cond_destroy(&t->stage2_cv);
        pthread_mutex_destroy(&t->stage2_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        pthread_mutex_destroy(&t->commit_exec_lock);
        free(t);
        return NULL;
    }

    unsigned int nr = t->info.nr_zones;
    if (zbd_report_zones(t->fd, 0, 0, ZBD_RO_ALL, t->zones, &nr) != 0)
    {
        perror("zbd_report_zones");
        free(t->zones);
        free(t->zone_wp_bytes);
        free(t->zone_full);
        if (t->direct_fd >= 0)
            close(t->direct_fd);
        zbd_close(t->fd);
        pthread_cond_destroy(&t->stage2_cv);
        pthread_mutex_destroy(&t->stage2_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        pthread_mutex_destroy(&t->commit_exec_lock);
        free(t);
        return NULL;
    }

    for (uint32_t z = 0; z < t->info.nr_zones; z++)
    {
        atomic_store_explicit(&t->zone_wp_bytes[z], t->zones[z].wp, memory_order_relaxed);
        atomic_store_explicit(
            &t->zone_full[z],
            (t->zones[z].cond == ZBD_ZONE_COND_FULL) ? 1 : 0,
            memory_order_relaxed);
    }

    load_superblock(t);

    uint32_t initial_zone = DATA_ZONE_START;
    for (uint32_t z = DATA_ZONE_START; z < t->info.nr_zones; z++)
    {
        if (!atomic_load_explicit(&t->zone_full[z], memory_order_acquire))
        {
            initial_zone = z;
            break;
        }
    }
    atomic_store_explicit(&t->current_zone, initial_zone, memory_order_release);

    t->stage2_head = NULL;
    t->stage2_tail = NULL;
    atomic_store_explicit(&t->dirty, false, memory_order_release);
    atomic_store_explicit(&t->flusher_stop, false, memory_order_release);
    atomic_store_explicit(&t->stop_sync, false, memory_order_release);
    atomic_store_explicit(&t->stop_commit, false, memory_order_release);

    if (pthread_create(&t->commit_tid, NULL, txg_commit_main, t) != 0)
    {
        perror("pthread_create txg_commit_main");
        free(t->zones);
        free(t->zone_wp_bytes);
        free(t->zone_full);
        if (t->direct_fd >= 0)
            close(t->direct_fd);
        zbd_close(t->fd);
        pthread_cond_destroy(&t->stage2_cv);
        pthread_mutex_destroy(&t->stage2_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        pthread_mutex_destroy(&t->commit_exec_lock);
        free(t);
        return NULL;
    }

    if (pthread_create(&t->sync_tid, NULL, txg_batch_main, t) != 0)
    {
        perror("pthread_create txg_batch_main");
        atomic_store_explicit(&t->stop_commit, true, memory_order_release);
        pthread_mutex_lock(&t->stage2_lock);
        pthread_cond_broadcast(&t->stage2_cv);
        pthread_mutex_unlock(&t->stage2_lock);
        pthread_join(t->commit_tid, NULL);
        free(t->zones);
        free(t->zone_wp_bytes);
        free(t->zone_full);
        if (t->direct_fd >= 0)
            close(t->direct_fd);
        zbd_close(t->fd);
        pthread_cond_destroy(&t->stage2_cv);
        pthread_mutex_destroy(&t->stage2_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }

    if (pthread_create(&t->flusher_tid, NULL, sb_flusher_thread, t) != 0)
    {
        perror("pthread_create flusher");
        atomic_store_explicit(&t->stop_sync, true, memory_order_release);
        pthread_mutex_lock(&t->q_lock);
        pthread_cond_broadcast(&t->q_cv);
        pthread_mutex_unlock(&t->q_lock);
        pthread_join(t->sync_tid, NULL);
        atomic_store_explicit(&t->stop_commit, true, memory_order_release);
        pthread_mutex_lock(&t->stage2_lock);
        pthread_cond_broadcast(&t->stage2_cv);
        pthread_mutex_unlock(&t->stage2_lock);
        pthread_join(t->commit_tid, NULL);
        free(t->zones);
        free(t->zone_wp_bytes);
        free(t->zone_full);
        if (t->direct_fd >= 0)
            close(t->direct_fd);
        zbd_close(t->fd);
        pthread_cond_destroy(&t->stage2_cv);
        pthread_mutex_destroy(&t->stage2_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }

    return t;
}

void cow_close(cow_tree *t)
{
    if (!t)
        return;

    atomic_store_explicit(&t->stop_sync, true, memory_order_release);
    pthread_mutex_lock(&t->q_lock);
    pthread_cond_broadcast(&t->q_cv);
    pthread_mutex_unlock(&t->q_lock);
    pthread_join(t->sync_tid, NULL);

    atomic_store_explicit(&t->stop_commit, true, memory_order_release);
    pthread_mutex_lock(&t->stage2_lock);
    pthread_cond_broadcast(&t->stage2_cv);
    pthread_mutex_unlock(&t->stage2_lock);
    pthread_join(t->commit_tid, NULL);

    {
        uint64_t tl_hit = atomic_load_explicit(&t->stat_tl_cache_hit, memory_order_relaxed);
        uint64_t ram_hit = atomic_load_explicit(&t->stat_ram_cache_hit, memory_order_relaxed);
        uint64_t disk_reads = atomic_load_explicit(&t->stat_disk_reads, memory_order_relaxed);
        uint64_t appends = atomic_load_explicit(&t->stat_page_appends, memory_order_relaxed);

        uint64_t qwait_ins_ns = atomic_load_explicit(&t->stat_q_lock_wait_ns_insert, memory_order_relaxed);
        uint64_t qwait_ins_samples = atomic_load_explicit(&t->stat_q_lock_wait_samples_insert, memory_order_relaxed);
        uint64_t qwait_w_ns = atomic_load_explicit(&t->stat_q_lock_wait_ns_sync, memory_order_relaxed);
        uint64_t qwait_w_samples = atomic_load_explicit(&t->stat_q_lock_wait_samples_sync, memory_order_relaxed);

        uint64_t batches = atomic_load_explicit(&t->stat_batches, memory_order_relaxed);
        uint64_t batch_items = atomic_load_explicit(&t->stat_batch_items, memory_order_relaxed);
        uint64_t qdepth_samples = atomic_load_explicit(&t->stat_queue_depth_samples, memory_order_relaxed);
        uint64_t qdepth_sum = atomic_load_explicit(&t->stat_queue_depth_sum, memory_order_relaxed);
        uint64_t qdepth_max = atomic_load_explicit(&t->stat_queue_depth_max, memory_order_relaxed);
        uint64_t writer_empty_waits = atomic_load_explicit(&t->stat_sync_idle_waits, memory_order_relaxed);
        uint64_t overlay_nodes_sum = atomic_load_explicit(&t->stat_overlay_nodes_sum, memory_order_relaxed);
        uint64_t overlay_nodes_max = atomic_load_explicit(&t->stat_overlay_nodes_max, memory_order_relaxed);
        uint64_t append_retries = atomic_load_explicit(&t->stat_append_retries, memory_order_relaxed);
        uint64_t zone_rotations = atomic_load_explicit(&t->stat_zone_rotations, memory_order_relaxed);
        uint64_t append_lat_ns_sum = atomic_load_explicit(&t->stat_append_latency_ns_sum, memory_order_relaxed);
        uint64_t append_lat_ns_samples = atomic_load_explicit(&t->stat_append_latency_ns_samples, memory_order_relaxed);
        uint64_t batch_lat_ns_sum = atomic_load_explicit(&t->stat_batch_latency_ns_sum, memory_order_relaxed);
        uint64_t batch_lat_ns_samples = atomic_load_explicit(&t->stat_batch_latency_ns_samples, memory_order_relaxed);
        uint64_t sort_lat_ns_sum = atomic_load_explicit(&t->stat_sort_latency_ns_sum, memory_order_relaxed);
        uint64_t sort_lat_ns_samples = atomic_load_explicit(&t->stat_sort_latency_ns_samples, memory_order_relaxed);
        uint64_t apply_lat_ns_sum = atomic_load_explicit(&t->stat_apply_latency_ns_sum, memory_order_relaxed);
        uint64_t apply_lat_ns_samples = atomic_load_explicit(&t->stat_apply_latency_ns_samples, memory_order_relaxed);
        uint64_t flush_lat_ns_sum = atomic_load_explicit(&t->stat_flush_latency_ns_sum, memory_order_relaxed);
        uint64_t flush_lat_ns_samples = atomic_load_explicit(&t->stat_flush_latency_ns_samples, memory_order_relaxed);

        double qwait_ins_avg_us = (qwait_ins_samples > 0)
                                      ? ((double)qwait_ins_ns / (double)qwait_ins_samples / 1000.0)
                                      : 0.0;
        double qwait_w_avg_us = (qwait_w_samples > 0)
                                    ? ((double)qwait_w_ns / (double)qwait_w_samples / 1000.0)
                                    : 0.0;
        double avg_batch_size = (batches > 0) ? ((double)batch_items / (double)batches) : 0.0;
        double avg_qdepth = (qdepth_samples > 0) ? ((double)qdepth_sum / (double)qdepth_samples) : 0.0;
        double avg_overlay_nodes = (batches > 0) ? ((double)overlay_nodes_sum / (double)batches) : 0.0;
        double append_lat_avg_us = (append_lat_ns_samples > 0) ? ((double)append_lat_ns_sum / (double)append_lat_ns_samples / 1000.0) : 0.0;
        double batch_lat_avg_us = (batch_lat_ns_samples > 0) ? ((double)batch_lat_ns_sum / (double)batch_lat_ns_samples / 1000.0) : 0.0;
        double sort_lat_avg_us = (sort_lat_ns_samples > 0) ? ((double)sort_lat_ns_sum / (double)sort_lat_ns_samples / 1000.0) : 0.0;
        double apply_lat_avg_us = (apply_lat_ns_samples > 0) ? ((double)apply_lat_ns_sum / (double)apply_lat_ns_samples / 1000.0) : 0.0;
        double flush_lat_avg_us = (flush_lat_ns_samples > 0) ? ((double)flush_lat_ns_sum / (double)flush_lat_ns_samples / 1000.0) : 0.0;

        fprintf(stderr,
                "[ram_stage2] page_cache tl_hit=%lu ram_hit=%lu disk_reads=%lu\n"
                "[ram_stage2] q_lock_wait_avg_us insert=%.2f sync=%.2f\n"
                "[ram_stage2] avg_batch_size=%.2f batches=%lu items=%lu\n"
                "[ram_stage2] queue_depth avg=%.2f max=%lu sync_idle_waits=%lu\n"
                "[ram_stage2] overlay_nodes avg=%.2f max=%lu\n"
                "[ram_stage2] append_retries=%lu zone_rotations=%lu\n"
                "[ram_stage2] sampled_latency_us append=%.2f txg=%.2f\n"
                "[ram_stage2] sampled_stage_us sort=%.2f apply=%.2f flush=%.2f\n"
                "[ram_stage2] page_appends=%lu\n",
                (unsigned long)tl_hit,
                (unsigned long)ram_hit,
                (unsigned long)disk_reads,
                qwait_ins_avg_us,
                qwait_w_avg_us,
                avg_batch_size,
                (unsigned long)batches,
                (unsigned long)batch_items,
            avg_qdepth,
            (unsigned long)qdepth_max,
            (unsigned long)writer_empty_waits,
            avg_overlay_nodes,
            (unsigned long)overlay_nodes_max,
            (unsigned long)append_retries,
            (unsigned long)zone_rotations,
            append_lat_avg_us,
            batch_lat_avg_us,
                sort_lat_avg_us,
                apply_lat_avg_us,
                flush_lat_avg_us,
                (unsigned long)appends);
    }

    atomic_store_explicit(&t->flusher_stop, true, memory_order_release);
    pthread_join(t->flusher_tid, NULL);

    if (atomic_exchange_explicit(&t->dirty, false, memory_order_acq_rel))
    {
        pagenum_t root;
        uint64_t seq;
        read_tree_snapshot(t, &root, &seq);

        pthread_mutex_lock(&t->flush_lock);
        t->durable_sb.root_pn = root;
        t->durable_sb.seq_no = seq / 2;
        pthread_mutex_unlock(&t->flush_lock);

        write_superblock_sync(t);
    }

    if (t->direct_fd >= 0)
        close(t->direct_fd);
    ram_table_destroy(t);
    free(t->zones);
    free(t->zone_wp_bytes);
    free(t->zone_full);
    zbd_close(t->fd);

    pthread_cond_destroy(&t->q_cv);
    pthread_cond_destroy(&t->stage2_cv);
    pthread_mutex_destroy(&t->stage2_lock);
    pthread_mutex_destroy(&t->commit_exec_lock);
    pthread_mutex_destroy(&t->q_lock);
    pthread_mutex_destroy(&t->flush_lock);

    free(t);
}

record *cow_find(cow_tree *t, int64_t key)
{
    pagenum_t root;
    uint64_t seq;
    read_tree_snapshot(t, &root, &seq);
    (void)seq;

    if (is_empty_snapshot(root))
        return NULL;

    page p;
    load_page(t, root, &p);

    while (!p.is_leaf)
    {
        pagenum_t child = p.pointer;
        for (uint32_t i = 0; i < p.num_keys; i++)
        {
            if (key < (int64_t)p.internal[i].key)
            {
                child = p.internal[i].child;
                break;
            }
        }
        load_page(t, child, &p);
    }

    for (uint32_t i = 0; i < p.num_keys; i++)
    {
        if ((int64_t)p.leaf[i].key == key)
        {
            record *r = malloc(sizeof *r);
            if (!r)
                return NULL;
            *r = p.leaf[i].record;
            return r;
        }
    }

    return NULL;
}

void cow_insert(cow_tree *t, int64_t key, const char *value)
{
    insert_req *req = get_tls_insert_req();
    req->key = key;
    memcpy(req->value, value, 120);
    req->done = 0;
    req->next = NULL;

    uint64_t qwait_start = monotonic_ns();
    pthread_mutex_lock(&t->q_lock);
    uint64_t qwait_end = monotonic_ns();
    atomic_fetch_add_explicit(&t->stat_q_lock_wait_ns_insert, qwait_end - qwait_start, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_q_lock_wait_samples_insert, 1, memory_order_relaxed);
    if (t->q_tail)
        t->q_tail->next = req;
    else
        t->q_head = req;
    t->q_tail = req;
    uint64_t qd = atomic_fetch_add_explicit(&t->stat_queue_depth_current, 1, memory_order_relaxed) + 1;
    if (prof_should_sample())
    {
        atomic_fetch_add_explicit(&t->stat_queue_depth_sum, qd, memory_order_relaxed);
        atomic_fetch_add_explicit(&t->stat_queue_depth_samples, 1, memory_order_relaxed);
        stat_update_max_u64(&t->stat_queue_depth_max, qd);
    }
    pthread_cond_signal(&t->q_cv);
    pthread_mutex_unlock(&t->q_lock);

    pthread_mutex_lock(&req->done_lock);
    while (!req->done)
    {
        pthread_cond_wait(&req->done_cv, &req->done_lock);
    }
    pthread_mutex_unlock(&req->done_lock);
}
