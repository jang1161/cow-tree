#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "cow_bt.h"

#define MAX_HEIGHT 32
#define RIGHTMOST_IDX UINT32_MAX

#define WRITER_BATCH_MAX 128
#define WRITER_BATCH_WAIT_US 100
#define WRITER_BATCH_MIN_WAIT_US 20

#define READ_CACHE_SLOTS 64
#define RAM_TABLE_INIT_CAP 65536
#define REF_TABLE_INIT_CAP 131072
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
    uint32_t source_refcnt;
    uint8_t cow_shared;
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

static pthread_key_t direct_read_buf_key;
static pthread_once_t direct_read_buf_key_once = PTHREAD_ONCE_INIT;
static pthread_key_t req_tls_key;
static pthread_once_t req_tls_key_once = PTHREAD_ONCE_INIT;
static __thread read_cache_entry tl_read_cache[READ_CACHE_SLOTS];

static inline uint64_t monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

typedef struct
{
    uint8_t used;
    pagenum_t pn;
    page *data;
} ram_page_slot;

typedef struct
{
    uint8_t used;
    pagenum_t pn;
    uint32_t refcnt;
} ref_slot;

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

static inline ref_slot *ref_slots(cow_tree *t)
{
    return (ref_slot *)t->ref_slots;
}

static void ref_table_init(cow_tree *t)
{
    t->ref_cap = REF_TABLE_INIT_CAP;
    t->ref_used = 0;
    t->ref_slots = calloc(t->ref_cap, sizeof(ref_slot));
    if (!t->ref_slots)
    {
        perror("calloc ref table");
        exit(EXIT_FAILURE);
    }
}

static void ref_table_destroy(cow_tree *t)
{
    free(t->ref_slots);
    t->ref_slots = NULL;
    t->ref_cap = 0;
    t->ref_used = 0;
}

static void ref_table_grow(cow_tree *t)
{
    size_t old_cap = t->ref_cap;
    ref_slot *old_slots = ref_slots(t);

    size_t new_cap = old_cap << 1;
    ref_slot *new_slots = calloc(new_cap, sizeof(ref_slot));
    if (!new_slots)
    {
        perror("calloc ref table grow");
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < old_cap; i++)
    {
        if (!old_slots[i].used)
            continue;

        size_t pos = (size_t)(page_hash_u64((uint64_t)old_slots[i].pn) & (new_cap - 1));
        while (new_slots[pos].used)
            pos = (pos + 1) & (new_cap - 1);
        new_slots[pos] = old_slots[i];
    }

    free(old_slots);
    t->ref_slots = new_slots;
    t->ref_cap = new_cap;
}

static ref_slot *ref_table_find_slot(cow_tree *t, pagenum_t pn)
{
    if (!t->ref_slots)
        ref_table_init(t);

    ref_slot *slots = ref_slots(t);
    size_t pos = (size_t)(page_hash_u64((uint64_t)pn) & (t->ref_cap - 1));

    for (;;)
    {
        ref_slot *s = &slots[pos];
        if (!s->used || s->pn == pn)
            return s;
        pos = (pos + 1) & (t->ref_cap - 1);
    }
}

static uint32_t fs_ref_get(cow_tree *t, pagenum_t pn)
{
    pthread_mutex_lock(&t->ref_lock);
    ref_slot *s = ref_table_find_slot(t, pn);
    uint32_t rc = (s->used) ? s->refcnt : 1;
    pthread_mutex_unlock(&t->ref_lock);
    return rc;
}

static void fs_ref_set(cow_tree *t, pagenum_t pn, uint32_t rc)
{
    pthread_mutex_lock(&t->ref_lock);

    if ((t->ref_used + 1) * 10 >= t->ref_cap * 7)
        ref_table_grow(t);

    ref_slot *s = ref_table_find_slot(t, pn);
    if (!s->used)
    {
        s->used = 1;
        s->pn = pn;
        t->ref_used++;
    }
    s->refcnt = rc;

    pthread_mutex_unlock(&t->ref_lock);
}

static void fs_ref_inc(cow_tree *t, pagenum_t pn)
{
    pthread_mutex_lock(&t->ref_lock);

    if ((t->ref_used + 1) * 10 >= t->ref_cap * 7)
        ref_table_grow(t);

    ref_slot *s = ref_table_find_slot(t, pn);
    if (!s->used)
    {
        s->used = 1;
        s->pn = pn;
        s->refcnt = 2;
        t->ref_used++;
    }
    else
    {
        s->refcnt++;
    }

    pthread_mutex_unlock(&t->ref_lock);
}

static void fs_ref_dec(cow_tree *t, pagenum_t pn)
{
    pthread_mutex_lock(&t->ref_lock);

    if ((t->ref_used + 1) * 10 >= t->ref_cap * 7)
        ref_table_grow(t);

    ref_slot *s = ref_table_find_slot(t, pn);
    if (!s->used)
    {
        s->used = 1;
        s->pn = pn;
        s->refcnt = 0;
        t->ref_used++;
    }
    else if (s->refcnt > 0)
    {
        s->refcnt--;
    }

    pthread_mutex_unlock(&t->ref_lock);
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
    pthread_mutex_lock(&t->ram_lock);

    if (!t->ram_slots)
    {
        pthread_mutex_unlock(&t->ram_lock);
        return;
    }

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

    pthread_mutex_unlock(&t->ram_lock);
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
    pthread_mutex_lock(&t->ram_lock);

    if (!t->ram_slots)
    {
        pthread_mutex_unlock(&t->ram_lock);
        return 0;
    }

    ram_page_slot *slots = ram_slots(t);
    size_t pos = (size_t)(page_hash_u64((uint64_t)pn) & (t->ram_cap - 1));

    for (;;)
    {
        ram_page_slot *s = &slots[pos];
        if (!s->used)
        {
            pthread_mutex_unlock(&t->ram_lock);
            return 0;
        }
        if (s->pn == pn)
        {
            *dst = *s->data;
            pthread_mutex_unlock(&t->ram_lock);
            return 1;
        }
        pos = (pos + 1) & (t->ram_cap - 1);
    }
}

static void ram_table_insert(cow_tree *t, pagenum_t pn, const page *src)
{
    pthread_mutex_lock(&t->ram_lock);

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
            pthread_mutex_unlock(&t->ram_lock);
            return;
        }

        if (s->pn == pn)
        {
            *s->data = *src;
            pthread_mutex_unlock(&t->ram_lock);
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

static __attribute__((unused)) insert_req *get_tls_insert_req(void)
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
    *root_pn = atomic_load_explicit(&t->volatile_sb.root_pn, memory_order_acquire);
    if (seq_no)
        *seq_no = atomic_load_explicit(&t->volatile_sb.seq_no, memory_order_acquire);
}

static inline uint32_t zone_next(cow_tree *t, uint32_t zone_id)
{
    return (zone_id + 1 < t->info.nr_zones) ? (zone_id + 1) : t->info.nr_zones;
}

static uint32_t reserve_writable_zone(cow_tree *t)
{
    for (;;)
    {
        uint32_t cur = atomic_load_explicit(&t->current_zone, memory_order_acquire);
        if (cur < DATA_ZONE_START)
        {
            uint32_t expected = cur;
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, DATA_ZONE_START,
                memory_order_acq_rel, memory_order_acquire);
            cur = DATA_ZONE_START;
        }

        if (cur >= t->info.nr_zones)
        {
            fprintf(stderr, "zones exhausted\n");
            exit(EXIT_FAILURE);
        }

        if (atomic_load_explicit(&t->zone_full[cur], memory_order_acquire))
        {
            uint32_t expected = cur;
            uint32_t next = zone_next(t, cur);
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, next,
                memory_order_acq_rel, memory_order_acquire);
            continue;
        }

        return cur;
    }
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

static pagenum_t cow_append_page(cow_tree *t, page *p)
{
    const uint64_t page_bytes = PAGE_SIZE;
    uint64_t retry = 0;

    for (;;)
    {
        uint32_t zone_id = reserve_writable_zone(t);

        uint64_t old_wp = atomic_fetch_add_explicit(
            &t->zone_wp_bytes[zone_id], page_bytes, memory_order_acq_rel);
        uint64_t zone_end = t->zones[zone_id].start + t->zones[zone_id].capacity;

        if (old_wp + page_bytes > zone_end)
        {
            atomic_fetch_sub_explicit(&t->zone_wp_bytes[zone_id], page_bytes, memory_order_acq_rel);
            atomic_store_explicit(&t->zone_full[zone_id], 1, memory_order_release);

            uint32_t expected = zone_id;
            uint32_t next = zone_next(t, zone_id);
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, next,
                memory_order_acq_rel, memory_order_acquire);
            continue;
        }

        pagenum_t pn;
        uint64_t wp_bytes;
        if (zone_append_raw_nolock(t, zone_id, p, &pn, &wp_bytes) == 0)
        {
            atomic_fetch_add_explicit(&t->stat_page_appends, 1, memory_order_relaxed);
            uint64_t cur_wp = atomic_load_explicit(&t->zone_wp_bytes[zone_id], memory_order_acquire);
            while (wp_bytes > cur_wp &&
                   !atomic_compare_exchange_weak_explicit(
                       &t->zone_wp_bytes[zone_id], &cur_wp, wp_bytes,
                       memory_order_acq_rel, memory_order_acquire))
            {
            }

            if (wp_bytes >= zone_end)
            {
                atomic_store_explicit(&t->zone_full[zone_id], 1, memory_order_release);
                uint32_t expected = zone_id;
                uint32_t next = zone_next(t, zone_id);
                atomic_compare_exchange_weak_explicit(
                    &t->current_zone, &expected, next,
                    memory_order_acq_rel, memory_order_acquire);
            }

            p->pn = pn;
            ram_table_insert(t, pn, p);
            fs_ref_set(t, pn, 1);
            return pn;
        }

        atomic_store_explicit(&t->zone_full[zone_id], 1, memory_order_release);
        uint32_t expected = zone_id;
        uint32_t next = zone_next(t, zone_id);
        atomic_compare_exchange_weak_explicit(
            &t->current_zone, &expected, next,
            memory_order_acq_rel, memory_order_acquire);

        if (++retry > t->info.nr_zones)
        {
            perror("nvme_zns_append");
            fprintf(stderr, "append retry exceeded number of zones\n");
            exit(EXIT_FAILURE);
        }
    }
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
    atomic_store_explicit(&t->volatile_sb.seq_no, t->durable_sb.seq_no, memory_order_release);

    if (t->durable_sb.root_pn != INVALID_PGN)
        fs_ref_set(t, t->durable_sb.root_pn, 1);
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
        t->durable_sb.seq_no = seq;
        pthread_mutex_unlock(&t->flush_lock);

        write_superblock_sync(t);
    }

    return NULL;
}

static void publish_root_single_writer(cow_tree *t, pagenum_t new_root)
{
    atomic_store_explicit(&t->volatile_sb.root_pn, new_root, memory_order_release);
    atomic_fetch_add_explicit(&t->volatile_sb.seq_no, 1, memory_order_acq_rel);
    atomic_store_explicit(&t->dirty, true, memory_order_release);
}

static int publish_root_if_snapshot_matches(cow_tree *t,
                                            pagenum_t expected_root,
                                            uint64_t expected_seq,
                                            pagenum_t new_root)
{
    pagenum_t cur_root = atomic_load_explicit(&t->volatile_sb.root_pn, memory_order_acquire);
    uint64_t cur_seq = atomic_load_explicit(&t->volatile_sb.seq_no, memory_order_acquire);

    if (cur_root == expected_root && cur_seq == expected_seq)
    {
        publish_root_single_writer(t, new_root);
        return 1;
    }

    return 0;
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
    n->source_refcnt = 0;
    n->cow_shared = 0;
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
    overlay_node *n = overlay_add_node(ov, id, &p);

    uint32_t rc = fs_ref_get(ov->t, (pagenum_t)id);
    n->source_refcnt = rc;

    if (rc > 1)
    {
        n->cow_shared = 1;
        atomic_fetch_add_explicit(&ov->t->stat_cow_shared, 1, memory_order_relaxed);

        if (!p.is_leaf)
        {
            for (uint32_t i = 0; i < p.num_keys; i++)
            {
                if (p.internal[i].child != INVALID_PGN)
                    fs_ref_inc(ov->t, p.internal[i].child);
            }
            if (p.pointer != INVALID_PGN)
                fs_ref_inc(ov->t, p.pointer);
        }
    }
    else
    {
        atomic_fetch_add_explicit(&ov->t->stat_cow_unique, 1, memory_order_relaxed);
    }

    // 현재 writer가 old node 참조를 떼고 private copy를 만든 것으로 간주한다.
    fs_ref_dec(ov->t, (pagenum_t)id);

    n->dirty = 1;
    return n;
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

static pagenum_t flush_overlay_node(overlay_state *ov, node_id_t id)
{
    if (id == INVALID_PGN)
        return INVALID_PGN;

    if (!is_temp_id(id))
    {
        int ridx = overlay_find_idx(ov, id);
        if (ridx < 0)
            return (pagenum_t)id;
    }

    int idx = overlay_find_idx(ov, id);
    if (idx < 0)
    {
        fprintf(stderr, "overlay flush: node not found %lu\n", (unsigned long)id);
        exit(EXIT_FAILURE);
    }

    overlay_node *n = &ov->arr[idx];
    if (n->flushed)
        return n->flushed_pn;

    page out = n->node;
    int changed = 0;

    if (!out.is_leaf)
    {
        for (uint32_t i = 0; i < out.num_keys; i++)
        {
            pagenum_t old_child = out.internal[i].child;
            pagenum_t new_child = flush_overlay_node(ov, old_child);
            if (old_child != new_child)
                changed = 1;
            out.internal[i].child = new_child;
        }

        pagenum_t old_ptr = out.pointer;
        pagenum_t new_ptr = flush_overlay_node(ov, old_ptr);
        if (old_ptr != new_ptr)
            changed = 1;
        out.pointer = new_ptr;
    }

    if (n->dirty || changed || is_temp_id(n->id))
    {
        n->flushed_pn = cow_append_page(ov->t, &out);
    }
    else
    {
        n->flushed_pn = (pagenum_t)n->id;
    }

    n->flushed = 1;
    return n->flushed_pn;
}

static int pop_batch(cow_tree *t, insert_req **batch, int max_batch)
{
    int n = 0;

    uint64_t qwait_start = monotonic_ns();
    pthread_mutex_lock(&t->q_lock);
    uint64_t qwait_end = monotonic_ns();
    atomic_fetch_add_explicit(&t->stat_q_lock_wait_ns_writer, qwait_end - qwait_start, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_q_lock_wait_samples_writer, 1, memory_order_relaxed);

    while (!atomic_load_explicit(&t->stop_writer, memory_order_acquire) && t->q_head == NULL)
    {
        pthread_cond_wait(&t->q_cv, &t->q_lock);
    }

    if (atomic_load_explicit(&t->stop_writer, memory_order_acquire) && t->q_head == NULL)
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

    if (n < max_batch && !atomic_load_explicit(&t->stop_writer, memory_order_acquire))
    {
        if (n >= 4)
        {
            uint32_t wait_us = (n >= 16) ? WRITER_BATCH_WAIT_US : WRITER_BATCH_MIN_WAIT_US;

            struct timespec now;
            clock_gettime(CLOCK_REALTIME, &now);
            uint64_t deadline_ns = (uint64_t)now.tv_nsec + (uint64_t)wait_us * 1000ULL;
            struct timespec deadline = {
                .tv_sec = now.tv_sec + (time_t)(deadline_ns / 1000000000ULL),
                .tv_nsec = (long)(deadline_ns % 1000000000ULL)};

            for (;;)
            {
                if (n >= max_batch || atomic_load_explicit(&t->stop_writer, memory_order_acquire))
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

typedef struct
{
    insert_req *req;
    int ord;
} batch_item;

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

static __attribute__((unused)) void *writer_main(void *arg)
{
    cow_tree *t = (cow_tree *)arg;
    insert_req *batch[WRITER_BATCH_MAX];
    batch_item sorted[WRITER_BATCH_MAX];

    for (;;)
    {
        int n = pop_batch(t, batch, WRITER_BATCH_MAX);
        if (n == 0)
            break;

        atomic_fetch_add_explicit(&t->stat_batches, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&t->stat_batch_items, (uint64_t)n, memory_order_relaxed);

        pagenum_t root;
        uint64_t seq;
        read_tree_snapshot(t, &root, &seq);
        (void)seq;

        overlay_state ov;
        memset(&ov, 0, sizeof(ov));
        ov.t = t;

        for (int i = 0; i < n; i++)
        {
            sorted[i].req = batch[i];
            sorted[i].ord = i;
        }
        qsort(sorted, (size_t)n, sizeof(sorted[0]), cmp_batch_item);

        node_id_t root_id = root;
        for (int i = 0; i < n; i++)
        {
            apply_insert_overlay(&ov, &root_id, sorted[i].req->key, sorted[i].req->value);
        }

        pagenum_t new_root = (root_id == INVALID_PGN) ? INVALID_PGN : flush_overlay_node(&ov, root_id);
        free(ov.arr);
        free(ov.idx_table);

        publish_root_single_writer(t, new_root);

        for (int i = 0; i < n; i++)
        {
            complete_req(batch[i]);
        }
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
    if (pthread_rwlock_init(&t->tree_lock, NULL) != 0)
    {
        perror("pthread_rwlock_init tree_lock");
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }
    if (pthread_mutex_init(&t->publish_lock, NULL) != 0)
    {
        perror("pthread_mutex_init publish_lock");
        pthread_rwlock_destroy(&t->tree_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }
    if (pthread_mutex_init(&t->ref_lock, NULL) != 0)
    {
        perror("pthread_mutex_init ref_lock");
        pthread_mutex_destroy(&t->publish_lock);
        pthread_rwlock_destroy(&t->tree_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }
    if (pthread_mutex_init(&t->ram_lock, NULL) != 0)
    {
        perror("pthread_mutex_init ram_lock");
        pthread_mutex_destroy(&t->ref_lock);
        pthread_mutex_destroy(&t->publish_lock);
        pthread_rwlock_destroy(&t->tree_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }

    ref_table_init(t);

    t->fd = zbd_open(path, O_RDWR, &t->info);
    if (t->fd < 0)
    {
        perror("zbd_open");
        ref_table_destroy(t);
        pthread_mutex_destroy(&t->ram_lock);
        pthread_mutex_destroy(&t->ref_lock);
        pthread_mutex_destroy(&t->publish_lock);
        pthread_rwlock_destroy(&t->tree_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
        free(t);
        return NULL;
    }

    if (nvme_get_nsid(t->fd, &t->nsid) != 0)
    {
        perror("nvme_get_nsid");
        zbd_close(t->fd);
        ref_table_destroy(t);
        pthread_mutex_destroy(&t->ram_lock);
        pthread_mutex_destroy(&t->ref_lock);
        pthread_mutex_destroy(&t->publish_lock);
        pthread_rwlock_destroy(&t->tree_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
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
        ref_table_destroy(t);
        pthread_mutex_destroy(&t->ram_lock);
        pthread_mutex_destroy(&t->ref_lock);
        pthread_mutex_destroy(&t->publish_lock);
        pthread_rwlock_destroy(&t->tree_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
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
        ref_table_destroy(t);
        pthread_mutex_destroy(&t->ram_lock);
        pthread_mutex_destroy(&t->ref_lock);
        pthread_mutex_destroy(&t->publish_lock);
        pthread_rwlock_destroy(&t->tree_lock);
        pthread_cond_destroy(&t->q_cv);
        pthread_mutex_destroy(&t->q_lock);
        pthread_mutex_destroy(&t->flush_lock);
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

    atomic_store_explicit(&t->dirty, false, memory_order_release);
    atomic_store_explicit(&t->flusher_stop, false, memory_order_release);
    atomic_store_explicit(&t->stop_writer, false, memory_order_release);

    if (pthread_create(&t->flusher_tid, NULL, sb_flusher_thread, t) != 0)
    {
        perror("pthread_create flusher");
        free(t->zones);
        free(t->zone_wp_bytes);
        free(t->zone_full);
        if (t->direct_fd >= 0)
            close(t->direct_fd);
        zbd_close(t->fd);
        ref_table_destroy(t);
        pthread_mutex_destroy(&t->ram_lock);
        pthread_mutex_destroy(&t->ref_lock);
        pthread_mutex_destroy(&t->publish_lock);
        pthread_rwlock_destroy(&t->tree_lock);
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

    {
        uint64_t tl_hit = atomic_load_explicit(&t->stat_tl_cache_hit, memory_order_relaxed);
        uint64_t ram_hit = atomic_load_explicit(&t->stat_ram_cache_hit, memory_order_relaxed);
        uint64_t disk_reads = atomic_load_explicit(&t->stat_disk_reads, memory_order_relaxed);
        uint64_t appends = atomic_load_explicit(&t->stat_page_appends, memory_order_relaxed);

        uint64_t qwait_ins_ns = atomic_load_explicit(&t->stat_q_lock_wait_ns_insert, memory_order_relaxed);
        uint64_t qwait_ins_samples = atomic_load_explicit(&t->stat_q_lock_wait_samples_insert, memory_order_relaxed);
        uint64_t batches = atomic_load_explicit(&t->stat_batches, memory_order_relaxed);
        uint64_t batch_items = atomic_load_explicit(&t->stat_batch_items, memory_order_relaxed);

        double qwait_ins_avg_us = (qwait_ins_samples > 0)
                                      ? ((double)qwait_ins_ns / (double)qwait_ins_samples / 1000.0)
                                      : 0.0;
        double avg_batch_size = (batches > 0) ? ((double)batch_items / (double)batches) : 0.0;

        fprintf(stderr,
                "[cow-bt] page_cache tl_hit=%lu ram_hit=%lu disk_reads=%lu\n"
                "[cow-bt] insert_lock_wait_avg_us=%.2f\n"
                "[cow-bt] avg_batch_size=%.2f batches=%lu items=%lu\n"
                "[cow-bt] page_appends=%lu publish_retries=%lu cow_shared=%lu cow_unique=%lu\n",
                (unsigned long)tl_hit,
                (unsigned long)ram_hit,
                (unsigned long)disk_reads,
                qwait_ins_avg_us,
                avg_batch_size,
                (unsigned long)batches,
                (unsigned long)batch_items,
                (unsigned long)appends,
                (unsigned long)atomic_load_explicit(&t->stat_publish_retries, memory_order_relaxed),
                (unsigned long)atomic_load_explicit(&t->stat_cow_shared, memory_order_relaxed),
                (unsigned long)atomic_load_explicit(&t->stat_cow_unique, memory_order_relaxed));
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
        t->durable_sb.seq_no = seq;
        pthread_mutex_unlock(&t->flush_lock);

        write_superblock_sync(t);
    }

    if (t->direct_fd >= 0)
        close(t->direct_fd);
    ram_table_destroy(t);
    ref_table_destroy(t);
    free(t->zones);
    free(t->zone_wp_bytes);
    free(t->zone_full);
    zbd_close(t->fd);

    pthread_cond_destroy(&t->q_cv);
    pthread_mutex_destroy(&t->q_lock);
    pthread_mutex_destroy(&t->ram_lock);
    pthread_mutex_destroy(&t->ref_lock);
    pthread_mutex_destroy(&t->publish_lock);
    pthread_rwlock_destroy(&t->tree_lock);
    pthread_mutex_destroy(&t->flush_lock);

    free(t);
}

record *cow_find(cow_tree *t, int64_t key)
{
    pthread_rwlock_rdlock(&t->tree_lock);

    pagenum_t root;
    uint64_t seq;
    read_tree_snapshot(t, &root, &seq);
    (void)seq;

    if (is_empty_snapshot(root))
    {
        pthread_rwlock_unlock(&t->tree_lock);
        return NULL;
    }

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
            {
                pthread_rwlock_unlock(&t->tree_lock);
                return NULL;
            }
            *r = p.leaf[i].record;
            pthread_rwlock_unlock(&t->tree_lock);
            return r;
        }
    }

    pthread_rwlock_unlock(&t->tree_lock);
    return NULL;
}

typedef struct
{
    pagenum_t pn;
    page p;
    uint32_t cidx;
} bt_path_entry;

static void bt_mark_cow_node(cow_tree *t, pagenum_t old_pn, const page *old_p)
{
    uint32_t rc = fs_ref_get(t, old_pn);
    if (rc > 1)
    {
        atomic_fetch_add_explicit(&t->stat_cow_shared, 1, memory_order_relaxed);
        if (!old_p->is_leaf)
        {
            for (uint32_t i = 0; i < old_p->num_keys; i++)
            {
                if (old_p->internal[i].child != INVALID_PGN)
                    fs_ref_inc(t, old_p->internal[i].child);
            }
            if (old_p->pointer != INVALID_PGN)
                fs_ref_inc(t, old_p->pointer);
        }
    }
    else
    {
        atomic_fetch_add_explicit(&t->stat_cow_unique, 1, memory_order_relaxed);
    }

    fs_ref_dec(t, old_pn);
}

static pagenum_t bt_insert_once(cow_tree *t, pagenum_t root, int64_t key, const char *value)
{
    if (root == INVALID_PGN)
    {
        page leaf;
        memset(&leaf, 0, sizeof(leaf));
        leaf.is_leaf = 1;
        leaf.num_keys = 1;
        leaf.pointer = INVALID_PGN;
        leaf.leaf[0].key = (uint64_t)key;
        memcpy(leaf.leaf[0].record.value, value, 120);
        return cow_append_page(t, &leaf);
    }

    bt_path_entry path[MAX_HEIGHT];
    int depth = 0;
    pagenum_t cur_pn = root;

    for (;;)
    {
        if (depth >= MAX_HEIGHT)
        {
            fprintf(stderr, "tree depth exceeded MAX_HEIGHT\n");
            exit(EXIT_FAILURE);
        }

        page cur;
        load_page(t, cur_pn, &cur);
        path[depth].pn = cur_pn;
        path[depth].p = cur;
        path[depth].cidx = RIGHTMOST_IDX;
        depth++;

        if (cur.is_leaf)
            break;

        uint32_t idx = RIGHTMOST_IDX;
        pagenum_t child = cur.pointer;
        for (uint32_t i = 0; i < cur.num_keys; i++)
        {
            if (key < (int64_t)cur.internal[i].key)
            {
                idx = i;
                child = cur.internal[i].child;
                break;
            }
        }

        path[depth - 1].cidx = idx;
        cur_pn = child;
    }

    page cur_left = path[depth - 1].p;
    bt_mark_cow_node(t, path[depth - 1].pn, &cur_left);

    int carry_split = 0;
    int64_t carry_key = 0;
    page cur_right;
    memset(&cur_right, 0, sizeof(cur_right));

    int updated = 0;
    for (uint32_t i = 0; i < cur_left.num_keys; i++)
    {
        if ((int64_t)cur_left.leaf[i].key == key)
        {
            memcpy(cur_left.leaf[i].record.value, value, 120);
            updated = 1;
            break;
        }
    }

    if (!updated)
    {
        if (cur_left.num_keys < LEAF_ORDER - 1)
        {
            uint32_t pos = get_position(&cur_left, key);
            for (int64_t i = (int64_t)cur_left.num_keys - 1; i >= (int64_t)pos; i--)
                cur_left.leaf[i + 1] = cur_left.leaf[i];

            cur_left.leaf[pos].key = (uint64_t)key;
            memcpy(cur_left.leaf[pos].record.value, value, 120);
            cur_left.num_keys++;
        }
        else
        {
            leaf_entity tmp[LEAF_ORDER];
            uint32_t pos = 0;
            while (pos < cur_left.num_keys && (int64_t)cur_left.leaf[pos].key < key)
                pos++;

            for (uint32_t i = 0; i < pos; i++)
                tmp[i] = cur_left.leaf[i];
            for (uint32_t i = pos; i < cur_left.num_keys; i++)
                tmp[i + 1] = cur_left.leaf[i];

            tmp[pos].key = (uint64_t)key;
            memcpy(tmp[pos].record.value, value, 120);

            uint32_t sp = LEAF_ORDER / 2;
            for (uint32_t i = 0; i < sp; i++)
                cur_left.leaf[i] = tmp[i];
            cur_left.num_keys = sp;

            memset(&cur_right, 0, sizeof(cur_right));
            cur_right.is_leaf = 1;
            cur_right.num_keys = LEAF_ORDER - sp;
            for (uint32_t i = 0; i < cur_right.num_keys; i++)
                cur_right.leaf[i] = tmp[sp + i];

            cur_right.pointer = cur_left.pointer;
            carry_split = 1;
            carry_key = (int64_t)cur_right.leaf[0].key;
        }
    }

    pagenum_t new_right_pn = INVALID_PGN;
    if (carry_split)
        new_right_pn = cow_append_page(t, &cur_right);

    cur_left.pointer = carry_split ? new_right_pn : cur_left.pointer;
    pagenum_t new_left_pn = cow_append_page(t, &cur_left);

    for (int lvl = depth - 2; lvl >= 0; lvl--)
    {
        page par = path[lvl].p;
        bt_mark_cow_node(t, path[lvl].pn, &par);

        uint32_t cidx = path[lvl].cidx;
        uint32_t pos = (cidx == RIGHTMOST_IDX) ? par.num_keys : cidx;

        if (cidx == RIGHTMOST_IDX)
            par.pointer = new_left_pn;
        else
            par.internal[cidx].child = new_left_pn;

        if (!carry_split)
        {
            new_left_pn = cow_append_page(t, &par);
            continue;
        }

        if (par.num_keys < INTERNAL_ORDER - 1)
        {
            for (int64_t j = (int64_t)par.num_keys - 1; j >= (int64_t)pos; j--)
                par.internal[j + 1] = par.internal[j];

            par.internal[pos].key = (uint64_t)carry_key;
            par.internal[pos].child = new_left_pn;

            if (pos == par.num_keys)
                par.pointer = new_right_pn;
            else
                par.internal[pos + 1].child = new_right_pn;

            par.num_keys++;
            carry_split = 0;
            new_left_pn = cow_append_page(t, &par);
            continue;
        }

        int64_t tkeys[INTERNAL_ORDER];
        pagenum_t tchld[INTERNAL_ORDER + 1];

        for (uint32_t j = 0; j < pos; j++)
            tkeys[j] = (int64_t)par.internal[j].key;
        tkeys[pos] = carry_key;
        for (uint32_t j = pos; j < INTERNAL_ORDER - 1; j++)
            tkeys[j + 1] = (int64_t)par.internal[j].key;

        for (uint32_t j = 0; j < pos; j++)
            tchld[j] = par.internal[j].child;
        tchld[pos] = new_left_pn;
        tchld[pos + 1] = new_right_pn;
        for (uint32_t j = pos + 1; j < INTERNAL_ORDER; j++)
            tchld[j + 1] = (j < INTERNAL_ORDER - 1) ? par.internal[j].child : par.pointer;

        uint32_t sp = (INTERNAL_ORDER + 1) / 2;
        int64_t up_key = tkeys[sp - 1];

        for (uint32_t j = 0; j < sp - 1; j++)
        {
            par.internal[j].key = (uint64_t)tkeys[j];
            par.internal[j].child = tchld[j];
        }
        par.pointer = tchld[sp - 1];
        par.num_keys = sp - 1;

        page right;
        memset(&right, 0, sizeof(right));
        right.is_leaf = 0;
        for (uint32_t j = sp; j < INTERNAL_ORDER; j++)
        {
            right.internal[j - sp].key = (uint64_t)tkeys[j];
            right.internal[j - sp].child = tchld[j];
        }
        right.pointer = tchld[INTERNAL_ORDER];
        right.num_keys = INTERNAL_ORDER - sp;

        new_left_pn = cow_append_page(t, &par);
        new_right_pn = cow_append_page(t, &right);
        carry_split = 1;
        carry_key = up_key;
    }

    if (carry_split)
    {
        page new_root;
        memset(&new_root, 0, sizeof(new_root));
        new_root.is_leaf = 0;
        new_root.num_keys = 1;
        new_root.internal[0].key = (uint64_t)carry_key;
        new_root.internal[0].child = new_left_pn;
        new_root.pointer = new_right_pn;
        return cow_append_page(t, &new_root);
    }

    return new_left_pn;
}

void cow_insert(cow_tree *t, int64_t key, const char *value)
{
    pthread_rwlock_rdlock(&t->tree_lock);

    for (;;)
    {
        pagenum_t root;
        uint64_t seq;
        read_tree_snapshot(t, &root, &seq);

        if (root != INVALID_PGN)
            fs_ref_inc(t, root);

        pagenum_t new_root = bt_insert_once(t, root, key, value);

        uint64_t lock_wait_start = monotonic_ns();
        pthread_mutex_lock(&t->publish_lock);
        uint64_t lock_wait_end = monotonic_ns();
        atomic_fetch_add_explicit(&t->stat_q_lock_wait_ns_insert, lock_wait_end - lock_wait_start, memory_order_relaxed);
        atomic_fetch_add_explicit(&t->stat_q_lock_wait_samples_insert, 1, memory_order_relaxed);

        if (publish_root_if_snapshot_matches(t, root, seq, new_root))
        {
            pthread_mutex_unlock(&t->publish_lock);
            atomic_fetch_add_explicit(&t->stat_batches, 1, memory_order_relaxed);
            atomic_fetch_add_explicit(&t->stat_batch_items, 1, memory_order_relaxed);
            break;
        }

        pthread_mutex_unlock(&t->publish_lock);
        atomic_fetch_add_explicit(&t->stat_publish_retries, 1, memory_order_relaxed);
    }

    pthread_rwlock_unlock(&t->tree_lock);
}
