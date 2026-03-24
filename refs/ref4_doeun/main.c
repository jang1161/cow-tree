#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <linux/blkzoned.h>
#include <linux/fs.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <limits.h>

static bool env_enabled(const char *name) {
    const char *v = getenv(name);
    return v && v[0] && strcmp(v, "0") != 0;
}

static void maybe_print_sysfs_value(const char *devname, const char *relpath, const char *label) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "/sys/class/block/%s/%s", devname, relpath);
    FILE *fp = fopen(path, "r");
    if (!fp) {
        fprintf(stderr, "[debug] sysfs %s: <unavailable> (%s)\n", label, path);
        return;
    }
    char buf[256];
    if (!fgets(buf, sizeof(buf), fp)) {
        fclose(fp);
        fprintf(stderr, "[debug] sysfs %s: <read-failed> (%s)\n", label, path);
        return;
    }
    fclose(fp);
    size_t n = strlen(buf);
    while (n && (buf[n-1] == '\n' || buf[n-1] == '\r')) buf[--n] = 0;
    fprintf(stderr, "[debug] sysfs %s=%s (%s)\n", label, buf, path);
}

static const char *zone_type_name(uint8_t type) {
    switch (type) {
        case BLK_ZONE_TYPE_CONVENTIONAL: return "conventional";
        case BLK_ZONE_TYPE_SEQWRITE_REQ: return "seq_req";
        case BLK_ZONE_TYPE_SEQWRITE_PREF: return "seq_pref";
        default: return "unknown";
    }
}

static const char *zone_cond_name(uint8_t cond) {
    switch (cond) {
        case BLK_ZONE_COND_NOT_WP: return "not_wp";
        case BLK_ZONE_COND_EMPTY: return "empty";
        case BLK_ZONE_COND_IMP_OPEN: return "imp_open";
        case BLK_ZONE_COND_EXP_OPEN: return "exp_open";
        case BLK_ZONE_COND_CLOSED: return "closed";
        case BLK_ZONE_COND_READONLY: return "readonly";
        case BLK_ZONE_COND_FULL: return "full";
        case BLK_ZONE_COND_OFFLINE: return "offline";
        default: return "unknown";
    }
}

static const char *dev_basename(const char *path) {
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}


#define PAGE_SIZE_BYTES 4096u
#define DEFAULT_DEVICE "/dev/nvme3n2"
#define NODE_MAGIC 0x434f5754u /* COWT */
#define CHECKPOINT_MAGIC 0x43504b54u /* CPKT */

/* 4KiB payload capacities kept close to the previous benchmark version. */
#define INTERNAL_MAX 248
#define LEAF_MAX     248

typedef uint64_t Key;
typedef uint64_t Val;

typedef struct Node Node;

typedef struct {
    uint8_t is_leaf;
    uint8_t _pad1;
    uint16_t nkeys;
    uint32_t _pad2;
    uint64_t gen;
} NodeHdr;

struct Node {
    NodeHdr h;
    uint32_t refcnt;
    uint32_t disk_zone_idx;
    uint64_t disk_offset_bytes; /* absolute byte offset on the raw device */
    union {
        struct {
            Key keys[INTERNAL_MAX];
            Node *child[INTERNAL_MAX + 1];
        } in;
        struct {
            Key keys[LEAF_MAX];
            Val vals[LEAF_MAX];
        } lf;
    };
};

typedef struct {
    Node *node;
    bool split;
    Key sep;
    Node *right;
} InsertRet;

typedef struct {
    uint64_t start_sector;
    uint64_t len_sectors;
    uint64_t capacity_sectors;
    uint64_t wp_sector;
    uint8_t type;
    uint8_t cond;
    bool in_use;
    uint64_t live_pages;
    uint32_t attached_writers;
} ZoneInfo;

typedef struct {
    int fd;
    char path[256];
    uint32_t logical_block_size;
    uint32_t physical_block_size;
    uint64_t zone_size_bytes;
    uint64_t zone_capacity_bytes;
    uint64_t device_size_bytes;
    uint32_t nr_zones;
    ZoneInfo *zones;
    uint32_t nr_seq_zones;
    uint32_t *seq_zone_ids;
    uint32_t next_free_seq_idx;
    uint32_t max_open_zones;
    uint32_t max_active_zones;
    pthread_mutex_t alloc_mu;
} ZnsDevice;

typedef struct {
    uint8_t bytes[PAGE_SIZE_BYTES];
} RawPage;


typedef struct {
    uint32_t zone_idx;
    uint64_t next_bytes_in_zone;
} ZoneWriter;

typedef struct {
    Node *root;
    uint64_t next_gen;
    uint64_t allocated_nodes;
    uint64_t freed_nodes;
    uint64_t logical_cow_bytes;
    uint64_t device_bytes_written;
    uint64_t persisted_pages;
    uint64_t checkpoint_pages;
    pthread_mutex_t mu;
    void *page_buf;
    ZnsDevice *zdev;
    ZoneWriter writer;
    uint32_t shard_id;
    uint64_t inserts;
} Tree;

typedef struct {
    Tree *shards;
    uint32_t nshards;
    ZnsDevice *zdev;
} Index;

static void zns_debug_dump(ZnsDevice *zd);
static void zns_debug_dump_zone(ZnsDevice *zd, uint32_t idx, const char *tag);
static void zns_note_page_dead(ZnsDevice *zd, uint32_t zone_idx);

typedef struct {
    Index *idx;
    uint64_t seed;
    uint64_t nops;
    uint64_t tid;
} WorkerArg;

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void die_errno(const char *msg) {
    perror(msg);
    exit(1);
}

static void *xaligned_alloc(size_t align, size_t sz) {
    void *p = NULL;
    if (posix_memalign(&p, align, sz) != 0 || !p) {
        fprintf(stderr, "posix_memalign(%zu, %zu) failed\n", align, sz);
        exit(1);
    }
    memset(p, 0, sz);
    return p;
}

static uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

static uint64_t xorshift64(uint64_t *s) {
    uint64_t x = *s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *s = x;
    return x;
}

static inline uint64_t sectors_to_bytes(uint64_t sectors) {
    return sectors * 512ULL;
}

static inline uint64_t bytes_to_sectors(uint64_t bytes) {
    return bytes / 512ULL;
}

static void node_incref(Node *n) {
    if (n) {
        assert(n->refcnt > 0);
        n->refcnt++;
    }
}

static void node_decref(Tree *t, Node *n) {
    if (!n) return;
    assert(n->refcnt > 0);
    n->refcnt--;
    if (n->refcnt > 0) return;
    if (!n->h.is_leaf) {
        for (uint32_t i = 0; i <= n->h.nkeys; ++i) {
            node_decref(t, n->in.child[i]);
        }
    }
    if (n->disk_offset_bytes != 0) {
        zns_note_page_dead(t->zdev, n->disk_zone_idx);
    }
    free(n);
    t->freed_nodes++;
}

static Node *alloc_node(Tree *t, bool is_leaf, uint64_t gen) {
    Node *n = (Node *)calloc(1, sizeof(Node));
    if (!n) die_errno("calloc node");
    n->h.is_leaf = is_leaf ? 1 : 0;
    n->h.nkeys = 0;
    n->h.gen = gen;
    n->refcnt = 1;
    t->allocated_nodes++;
    return n;
}

static int lower_bound_leaf(const Node *leaf, Key k) {
    int l = 0, r = leaf->h.nkeys;
    while (l < r) {
        int m = (l + r) >> 1;
        if (leaf->lf.keys[m] < k) l = m + 1;
        else r = m;
    }
    return l;
}

static int child_slot_internal(const Node *in, Key k) {
    int l = 0, r = in->h.nkeys;
    while (l < r) {
        int m = (l + r) >> 1;
        if (k < in->in.keys[m]) r = m;
        else l = m + 1;
    }
    return l;
}

static void zone_reset(int fd, uint64_t sector, uint64_t nr_sectors) {
    struct blk_zone_range range;
    memset(&range, 0, sizeof(range));
    range.sector = sector;
    range.nr_sectors = nr_sectors;
    if (ioctl(fd, BLKRESETZONE, &range) != 0) die_errno("BLKRESETZONE");
}

static void zone_open_if_supported(int fd, uint64_t sector, uint64_t nr_sectors) {
    struct blk_zone_range range;
    memset(&range, 0, sizeof(range));
    range.sector = sector;
    range.nr_sectors = nr_sectors;
    if (ioctl(fd, BLKOPENZONE, &range) != 0) {
        if (errno != EINVAL && errno != ENOTTY && errno != EOVERFLOW) {
            die_errno("BLKOPENZONE");
        }
    }
}

static void zone_finish_if_supported(int fd, uint64_t sector, uint64_t nr_sectors) {
    struct blk_zone_range range;
    memset(&range, 0, sizeof(range));
    range.sector = sector;
    range.nr_sectors = nr_sectors;
    if (ioctl(fd, BLKFINISHZONE, &range) != 0) {
        if (errno != EINVAL && errno != ENOTTY && errno != EOVERFLOW) {
            die_errno("BLKFINISHZONE");
        }
    }
}

static uint32_t read_sysfs_u32_or_zero(const char *devname, const char *relpath) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "/sys/block/%s/%s", devname, relpath);
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    unsigned long long v = 0;
    if (fscanf(f, "%llu", &v) != 1) {
        fclose(f);
        return 0;
    }
    fclose(f);
    return (uint32_t)v;
}

static void zns_init(ZnsDevice *zd, const char *path) {
    memset(zd, 0, sizeof(*zd));
    zd->fd = open(path, O_RDWR | O_DIRECT);
    if (zd->fd < 0) die_errno("open zns device");
    snprintf(zd->path, sizeof(zd->path), "%s", path);
    pthread_mutex_init(&zd->alloc_mu, NULL);

    if (ioctl(zd->fd, BLKSSZGET, &zd->logical_block_size) != 0) die_errno("BLKSSZGET");
    if (ioctl(zd->fd, BLKPBSZGET, &zd->physical_block_size) != 0) die_errno("BLKPBSZGET");
    if (ioctl(zd->fd, BLKGETSIZE64, &zd->device_size_bytes) != 0) die_errno("BLKGETSIZE64");
    if (ioctl(zd->fd, BLKGETNRZONES, &zd->nr_zones) != 0) die_errno("BLKGETNRZONES");

    uint32_t zone_sectors = 0;
    if (ioctl(zd->fd, BLKGETZONESZ, &zone_sectors) != 0) die_errno("BLKGETZONESZ");
    zd->zone_size_bytes = sectors_to_bytes(zone_sectors);

    zd->zones = (ZoneInfo *)calloc(zd->nr_zones, sizeof(ZoneInfo));
    zd->seq_zone_ids = (uint32_t *)calloc(zd->nr_zones, sizeof(uint32_t));
    if (!zd->zones || !zd->seq_zone_ids) die_errno("calloc zones");

    uint64_t sector = 0;
    uint32_t filled = 0;
    while (filled < zd->nr_zones) {
        uint32_t batch = 256;
        if (batch > zd->nr_zones - filled) batch = zd->nr_zones - filled;
        size_t rep_sz = sizeof(struct blk_zone_report) + (size_t)batch * sizeof(struct blk_zone);
        struct blk_zone_report *rep = (struct blk_zone_report *)calloc(1, rep_sz);
        if (!rep) die_errno("calloc zone report");
        rep->sector = sector;
        rep->nr_zones = batch;
        if (ioctl(zd->fd, BLKREPORTZONE, rep) != 0) die_errno("BLKREPORTZONE");
        if (rep->nr_zones == 0) {
            free(rep);
            break;
        }
        for (uint32_t i = 0; i < rep->nr_zones; ++i) {
            const struct blk_zone *z = &rep->zones[i];
            ZoneInfo *dst = &zd->zones[filled];
            dst->start_sector = z->start;
            dst->len_sectors = z->len;
            dst->capacity_sectors = z->capacity ? z->capacity : z->len;
            dst->wp_sector = z->wp;
            dst->type = z->type;
            dst->cond = z->cond;
            dst->in_use = false;
            if (z->type == BLK_ZONE_TYPE_SEQWRITE_REQ || z->type == BLK_ZONE_TYPE_SEQWRITE_PREF) {
                zd->seq_zone_ids[zd->nr_seq_zones++] = filled;
            }
            filled++;
            sector = z->start + z->len;
        }
        free(rep);
    }

    if (zd->nr_seq_zones == 0) {
        fprintf(stderr, "No sequential zones found on %s\n", path);
        exit(1);
    }
    if (zd->physical_block_size == 0) zd->physical_block_size = zd->logical_block_size;
    if (PAGE_SIZE_BYTES % zd->physical_block_size != 0) {
        fprintf(stderr, "4KiB node size is not aligned to physical block size %u\n", zd->physical_block_size);
        exit(1);
    }

    uint32_t first_seq = zd->seq_zone_ids[0];
    zd->zone_capacity_bytes = sectors_to_bytes(zd->zones[first_seq].capacity_sectors);
    if (env_enabled("COWBT_DEBUG_ZONES")) zns_debug_dump(zd);
}

static void zns_destroy(ZnsDevice *zd) {
    if (zd->fd >= 0) close(zd->fd);
    free(zd->zones);
    free(zd->seq_zone_ids);
}

static void zns_debug_dump_zone(ZnsDevice *zd, uint32_t idx, const char *tag) {
    if (idx >= zd->nr_zones) {
        fprintf(stderr, "[debug] %s zone[%u]: <out-of-range> nr_zones=%u\n", tag, idx, zd->nr_zones);
        return;
    }
    ZoneInfo *z = &zd->zones[idx];
    fprintf(stderr,
            "[debug] %s zone[%u]: start_sector=%" PRIu64 " start_bytes=%" PRIu64
            " len_sectors=%" PRIu64 " (%" PRIu64 " bytes)"
            " capacity_sectors=%" PRIu64 " (%" PRIu64 " bytes)"
            " wp_sector=%" PRIu64 " wp_bytes=%" PRIu64
            " type=%s(%u) cond=%s(%u) in_use=%d\n",
            tag, idx,
            z->start_sector, sectors_to_bytes(z->start_sector),
            z->len_sectors, sectors_to_bytes(z->len_sectors),
            z->capacity_sectors, sectors_to_bytes(z->capacity_sectors),
            z->wp_sector, sectors_to_bytes(z->wp_sector),
            zone_type_name(z->type), z->type,
            zone_cond_name(z->cond), z->cond,
            (int)z->in_use);
}

static void zns_debug_dump(ZnsDevice *zd) {
    const char *devname = dev_basename(zd->path);
    fprintf(stderr, "[debug] device=%s\n", zd->path);
    fprintf(stderr, "[debug] ioctl logical_block_size=%u\n", zd->logical_block_size);
    fprintf(stderr, "[debug] ioctl physical_block_size=%u\n", zd->physical_block_size);
    fprintf(stderr, "[debug] sizeof(void*)=%zu sizeof(off_t)=%zu sizeof(off64_t)=%zu\n", sizeof(void*), sizeof(off_t), sizeof(off64_t));
    fprintf(stderr, "[debug] ioctl device_size_bytes=%" PRIu64 "\n", zd->device_size_bytes);
    fprintf(stderr, "[debug] ioctl nr_zones=%u\n", zd->nr_zones);
    fprintf(stderr, "[debug] ioctl zone_size_bytes=%" PRIu64 "\n", zd->zone_size_bytes);
    fprintf(stderr, "[debug] first_seq_zone_capacity_bytes=%" PRIu64 "\n", zd->zone_capacity_bytes);

    maybe_print_sysfs_value(devname, "queue/zoned", "queue.zoned");
    maybe_print_sysfs_value(devname, "queue/nr_zones", "queue.nr_zones");
    maybe_print_sysfs_value(devname, "queue/chunk_sectors", "queue.chunk_sectors");
    maybe_print_sysfs_value(devname, "queue/max_open_zones", "queue.max_open_zones");
    maybe_print_sysfs_value(devname, "queue/max_active_zones", "queue.max_active_zones");
    maybe_print_sysfs_value(devname, "queue/zone_write_granularity", "queue.zone_write_granularity");

    zd->max_open_zones = read_sysfs_u32_or_zero(devname, "queue/max_open_zones");
    zd->max_active_zones = read_sysfs_u32_or_zero(devname, "queue/max_active_zones");

    uint32_t dump_n = zd->nr_zones < 8 ? zd->nr_zones : 8;
    fprintf(stderr, "[debug] dumping first %u reported zones\n", dump_n);
    for (uint32_t i = 0; i < dump_n; ++i) {
        ZoneInfo *z = &zd->zones[i];
        fprintf(stderr,
                "[debug] zone[%u]: start_sector=%" PRIu64
                " len_sectors=%" PRIu64 " (%" PRIu64 " bytes)"
                " capacity_sectors=%" PRIu64 " (%" PRIu64 " bytes)"
                " wp_sector=%" PRIu64 " type=%s(%u) cond=%s(%u)\n",
                i,
                z->start_sector,
                z->len_sectors, sectors_to_bytes(z->len_sectors),
                z->capacity_sectors, sectors_to_bytes(z->capacity_sectors),
                z->wp_sector,
                zone_type_name(z->type), z->type,
                zone_cond_name(z->cond), z->cond);
    }

    uint32_t seq_dump = zd->nr_seq_zones < 8 ? zd->nr_seq_zones : 8;
    fprintf(stderr, "[debug] first %u sequential zone ids:", seq_dump);
    for (uint32_t i = 0; i < seq_dump; ++i) fprintf(stderr, " %u", zd->seq_zone_ids[i]);
    fprintf(stderr, "\n");
    if (zd->nr_seq_zones > 0) {
        zns_debug_dump_zone(zd, zd->seq_zone_ids[0], "first-seq");
    }
}

static void zns_try_reclaim_zone_locked(ZnsDevice *zd, uint32_t zone_idx) {
    ZoneInfo *z = &zd->zones[zone_idx];
    if (!z->in_use) return;
    if (z->attached_writers != 0) return;
    if (z->live_pages != 0) return;
    zone_reset(zd->fd, z->start_sector, z->len_sectors);
    z->wp_sector = z->start_sector;
    z->cond = BLK_ZONE_COND_EMPTY;
    z->in_use = false;
}

static void zns_zone_attach_writer(ZnsDevice *zd, uint32_t zone_idx) {
    pthread_mutex_lock(&zd->alloc_mu);
    zd->zones[zone_idx].attached_writers++;
    pthread_mutex_unlock(&zd->alloc_mu);
}

static void zns_zone_detach_writer(ZnsDevice *zd, uint32_t zone_idx) {
    pthread_mutex_lock(&zd->alloc_mu);
    ZoneInfo *z = &zd->zones[zone_idx];
    if (z->attached_writers == 0) {
        pthread_mutex_unlock(&zd->alloc_mu);
        fprintf(stderr, "zone writer detach underflow on zone %u\n", zone_idx);
        exit(1);
    }
    z->attached_writers--;
    zns_try_reclaim_zone_locked(zd, zone_idx);
    pthread_mutex_unlock(&zd->alloc_mu);
}

static void zns_note_page_persisted(ZnsDevice *zd, uint32_t zone_idx) {
    pthread_mutex_lock(&zd->alloc_mu);
    zd->zones[zone_idx].live_pages++;
    pthread_mutex_unlock(&zd->alloc_mu);
}

static void zns_note_page_dead(ZnsDevice *zd, uint32_t zone_idx) {
    pthread_mutex_lock(&zd->alloc_mu);
    ZoneInfo *z = &zd->zones[zone_idx];
    if (z->live_pages == 0) {
        pthread_mutex_unlock(&zd->alloc_mu);
        fprintf(stderr, "zone live_pages underflow on zone %u\n", zone_idx);
        exit(1);
    }
    z->live_pages--;
    zns_try_reclaim_zone_locked(zd, zone_idx);
    pthread_mutex_unlock(&zd->alloc_mu);
}

static uint32_t zns_acquire_zone(ZnsDevice *zd) {
    pthread_mutex_lock(&zd->alloc_mu);
    uint32_t seq_rank = UINT32_MAX;
    uint32_t zone_idx = UINT32_MAX;
    if (zd->nr_seq_zones == 0) {
        pthread_mutex_unlock(&zd->alloc_mu);
        fprintf(stderr, "No sequential zones on %s\n", zd->path);
        exit(1);
    }
    uint32_t start = zd->next_free_seq_idx % zd->nr_seq_zones;
    for (uint32_t scanned = 0; scanned < zd->nr_seq_zones; ++scanned) {
        uint32_t rank = (start + scanned) % zd->nr_seq_zones;
        uint32_t cand = zd->seq_zone_ids[rank];
        if (!zd->zones[cand].in_use) {
            seq_rank = rank;
            zone_idx = cand;
            zd->zones[cand].in_use = true;
            zd->next_free_seq_idx = (rank + 1) % zd->nr_seq_zones;
            break;
        }
    }
    pthread_mutex_unlock(&zd->alloc_mu);

    if (zone_idx == UINT32_MAX) {
        fprintf(stderr, "No reusable sequential zones available on %s (need more reclaim headroom)\n", zd->path);
        exit(1);
    }

    fprintf(stderr, "[debug] acquiring seq zone rank=%u global_zone_idx=%u\n", seq_rank, zone_idx);
    zns_debug_dump_zone(zd, zone_idx, "acquire-before-reset");
    zone_reset(zd->fd, zd->zones[zone_idx].start_sector, zd->zones[zone_idx].len_sectors);
    zone_open_if_supported(zd->fd, zd->zones[zone_idx].start_sector, zd->zones[zone_idx].len_sectors);
    pthread_mutex_lock(&zd->alloc_mu);
    zd->zones[zone_idx].wp_sector = zd->zones[zone_idx].start_sector;
    zd->zones[zone_idx].cond = BLK_ZONE_COND_EMPTY;
    zd->zones[zone_idx].live_pages = 0;
    pthread_mutex_unlock(&zd->alloc_mu);
    zns_debug_dump_zone(zd, zone_idx, "acquire-after-reset");
    return zone_idx;
}

static void tree_acquire_initial_zone(Tree *t) {
    t->writer.zone_idx = zns_acquire_zone(t->zdev);
    zns_zone_attach_writer(t->zdev, t->writer.zone_idx);
    t->writer.next_bytes_in_zone = 0;
}

static uint64_t tree_append_page(Tree *t, const void *buf) {
    ZnsDevice *zd = t->zdev;
    ZoneInfo *z = &zd->zones[t->writer.zone_idx];
    uint64_t zone_capacity_bytes = sectors_to_bytes(z->capacity_sectors);
    if (t->writer.next_bytes_in_zone + PAGE_SIZE_BYTES > zone_capacity_bytes) {
        uint32_t old_zone_idx = t->writer.zone_idx;
        ZoneInfo *oldz = &zd->zones[old_zone_idx];
        if (t->writer.next_bytes_in_zone == zone_capacity_bytes) {
            zone_finish_if_supported(zd->fd, oldz->start_sector, oldz->len_sectors);
            pthread_mutex_lock(&zd->alloc_mu);
            oldz->cond = BLK_ZONE_COND_FULL;
            pthread_mutex_unlock(&zd->alloc_mu);
        }
        zns_zone_detach_writer(zd, old_zone_idx);
        t->writer.zone_idx = zns_acquire_zone(zd);
        zns_zone_attach_writer(zd, t->writer.zone_idx);
        t->writer.next_bytes_in_zone = 0;
        z = &zd->zones[t->writer.zone_idx];
        zone_capacity_bytes = sectors_to_bytes(z->capacity_sectors);
        if (PAGE_SIZE_BYTES > zone_capacity_bytes) {
            fprintf(stderr, "Zone capacity too small for 4KiB writes\n");
            exit(1);
        }
    }

    uint64_t dev_off = sectors_to_bytes(z->start_sector) + t->writer.next_bytes_in_zone;
    ssize_t wr = pwrite64(zd->fd, buf, PAGE_SIZE_BYTES, (off64_t)dev_off);
    if (wr < 0) {
        fprintf(stderr,
                "pwrite64 zns failed: off=%" PRIu64 " zone_idx=%u zone_start_sector=%" PRIu64 " next_bytes=%" PRIu64
                " zone_len_bytes=%" PRIu64 " zone_capacity_bytes=%" PRIu64
                " dev_size_bytes=%" PRIu64
                " off%%logical=%" PRIu64 " off%%physical=%" PRIu64
                " page%%logical=%u page%%physical=%u errno=%d\n",
                dev_off, t->writer.zone_idx, z->start_sector, t->writer.next_bytes_in_zone,
                sectors_to_bytes(z->len_sectors), sectors_to_bytes(z->capacity_sectors),
                zd->device_size_bytes,
                dev_off % zd->logical_block_size, dev_off % zd->physical_block_size,
                PAGE_SIZE_BYTES % zd->logical_block_size, PAGE_SIZE_BYTES % zd->physical_block_size, errno);
        zns_debug_dump_zone(zd, t->writer.zone_idx, "write-failed-zone");
        die_errno("pwrite64 zns");
    }
    if ((size_t)wr != PAGE_SIZE_BYTES) {
        fprintf(stderr, "short pwrite: %zd\n", wr);
        exit(1);
    }

    t->writer.next_bytes_in_zone += PAGE_SIZE_BYTES;
    pthread_mutex_lock(&zd->alloc_mu);
    z->wp_sector = z->start_sector + bytes_to_sectors(t->writer.next_bytes_in_zone);
    pthread_mutex_unlock(&zd->alloc_mu);
    t->device_bytes_written += PAGE_SIZE_BYTES;
    t->persisted_pages++;
    return dev_off;
}

static void put_u16(uint8_t *dst, uint16_t v) { memcpy(dst, &v, sizeof(v)); }
static void put_u32(uint8_t *dst, uint32_t v) { memcpy(dst, &v, sizeof(v)); }
static void put_u64(uint8_t *dst, uint64_t v) { memcpy(dst, &v, sizeof(v)); }

static void persist_node(Tree *t, Node *n) {
    uint8_t *p = (uint8_t *)t->page_buf;
    memset(p, 0, PAGE_SIZE_BYTES);
    put_u32(p + 0, NODE_MAGIC);
    p[4] = n->h.is_leaf;
    put_u16(p + 8, n->h.nkeys);
    put_u64(p + 16, n->h.gen);

    if (n->h.is_leaf) {
        size_t off = 32;
        for (uint32_t i = 0; i < n->h.nkeys; ++i) {
            put_u64(p + off, n->lf.keys[i]);
            off += 8;
        }
        off = 32 + (size_t)LEAF_MAX * 8;
        for (uint32_t i = 0; i < n->h.nkeys; ++i) {
            put_u64(p + off, n->lf.vals[i]);
            off += 8;
        }
    } else {
        size_t off = 32;
        for (uint32_t i = 0; i < n->h.nkeys; ++i) {
            put_u64(p + off, n->in.keys[i]);
            off += 8;
        }
        off = 32 + (size_t)INTERNAL_MAX * 8;
        for (uint32_t i = 0; i <= n->h.nkeys; ++i) {
            assert(n->in.child[i] != NULL);
            assert(n->in.child[i]->disk_offset_bytes != 0);
            put_u64(p + off, n->in.child[i]->disk_offset_bytes);
            off += 8;
        }
    }
    n->disk_offset_bytes = tree_append_page(t, p);
    n->disk_zone_idx = t->writer.zone_idx;
    zns_note_page_persisted(t->zdev, n->disk_zone_idx);
}

static void persist_checkpoint(Tree *t, uint64_t total_ops) {
    uint8_t *p = (uint8_t *)t->page_buf;
    memset(p, 0, PAGE_SIZE_BYTES);
    put_u32(p + 0, CHECKPOINT_MAGIC);
    put_u32(p + 4, t->shard_id);
    put_u64(p + 8, t->root ? t->root->disk_offset_bytes : 0);
    put_u64(p + 16, t->root ? t->root->h.gen : 0);
    put_u64(p + 24, total_ops);
    (void)tree_append_page(t, p);
    t->checkpoint_pages++;
}

static Node *build_leaf_no_split(Tree *t, const Node *cur, Key k, Val v, int pos, uint64_t gen) {
    Node *n = alloc_node(t, true, gen);
    n->h.nkeys = cur->h.nkeys + 1;
    for (int i = 0; i < pos; ++i) {
        n->lf.keys[i] = cur->lf.keys[i];
        n->lf.vals[i] = cur->lf.vals[i];
    }
    n->lf.keys[pos] = k;
    n->lf.vals[pos] = v;
    for (int i = pos; i < cur->h.nkeys; ++i) {
        n->lf.keys[i + 1] = cur->lf.keys[i];
        n->lf.vals[i + 1] = cur->lf.vals[i];
    }
    persist_node(t, n);
    t->logical_cow_bytes += PAGE_SIZE_BYTES;
    return n;
}

static Node *build_leaf_update(Tree *t, const Node *cur, Key k, Val v, int pos, uint64_t gen) {
    Node *n = alloc_node(t, true, gen);
    n->h.nkeys = cur->h.nkeys;
    for (int i = 0; i < cur->h.nkeys; ++i) {
        n->lf.keys[i] = cur->lf.keys[i];
        n->lf.vals[i] = cur->lf.vals[i];
    }
    n->lf.keys[pos] = k;
    n->lf.vals[pos] = v;
    persist_node(t, n);
    t->logical_cow_bytes += PAGE_SIZE_BYTES;
    return n;
}

static InsertRet insert_rec(Tree *t, Node *cur, Key k, Val v, uint64_t gen) {
    if (cur->h.is_leaf) {
        int pos = lower_bound_leaf(cur, k);
        if (pos < cur->h.nkeys && cur->lf.keys[pos] == k) {
            Node *n = build_leaf_update(t, cur, k, v, pos, gen);
            return (InsertRet){ .node = n, .split = false, .sep = 0, .right = NULL };
        }

        if (cur->h.nkeys < LEAF_MAX) {
            Node *n = build_leaf_no_split(t, cur, k, v, pos, gen);
            return (InsertRet){ .node = n, .split = false, .sep = 0, .right = NULL };
        }

        Key tmpk[LEAF_MAX + 1];
        Val tmpv[LEAF_MAX + 1];
        int i = 0, j = 0;
        while (i < cur->h.nkeys) {
            if (j == pos) {
                tmpk[j] = k;
                tmpv[j] = v;
                j++;
            } else {
                tmpk[j] = cur->lf.keys[i];
                tmpv[j] = cur->lf.vals[i];
                i++; j++;
            }
        }
        if (j == pos) {
            tmpk[j] = k;
            tmpv[j] = v;
            j++;
        }
        assert(j == LEAF_MAX + 1);

        int mid = (LEAF_MAX + 1) / 2;
        Node *left = alloc_node(t, true, gen);
        Node *right = alloc_node(t, true, gen);
        left->h.nkeys = mid;
        right->h.nkeys = (LEAF_MAX + 1) - mid;

        for (i = 0; i < mid; ++i) {
            left->lf.keys[i] = tmpk[i];
            left->lf.vals[i] = tmpv[i];
        }
        for (i = 0; i < (int)right->h.nkeys; ++i) {
            right->lf.keys[i] = tmpk[mid + i];
            right->lf.vals[i] = tmpv[mid + i];
        }
        persist_node(t, left);
        persist_node(t, right);
        t->logical_cow_bytes += 2ULL * PAGE_SIZE_BYTES;
        return (InsertRet){ .node = left, .split = true, .sep = right->lf.keys[0], .right = right };
    }

    int slot = child_slot_internal(cur, k);
    InsertRet ch = insert_rec(t, cur->in.child[slot], k, v, gen);

    if (!ch.split && cur->h.nkeys < INTERNAL_MAX) {
        Node *n = alloc_node(t, false, gen);
        n->h.nkeys = cur->h.nkeys;
        for (int i = 0; i < cur->h.nkeys; ++i) n->in.keys[i] = cur->in.keys[i];
        for (int i = 0; i <= cur->h.nkeys; ++i) {
            if (i == slot) {
                n->in.child[i] = ch.node;
            } else {
                n->in.child[i] = cur->in.child[i];
                node_incref(n->in.child[i]);
            }
        }
        persist_node(t, n);
        t->logical_cow_bytes += PAGE_SIZE_BYTES;
        return (InsertRet){ .node = n, .split = false, .sep = 0, .right = NULL };
    }

    Key tmpk[INTERNAL_MAX + 1];
    Node *tmpc[INTERNAL_MAX + 2];

    if (!ch.split) {
        int nk = cur->h.nkeys;
        Node *n = alloc_node(t, false, gen);
        n->h.nkeys = nk;
        for (int i = 0; i < nk; ++i) n->in.keys[i] = cur->in.keys[i];
        for (int i = 0; i <= nk; ++i) {
            if (i == slot) {
                n->in.child[i] = ch.node;
            } else {
                n->in.child[i] = cur->in.child[i];
                node_incref(n->in.child[i]);
            }
        }
        persist_node(t, n);
        t->logical_cow_bytes += PAGE_SIZE_BYTES;
        return (InsertRet){ .node = n, .split = false, .sep = 0, .right = NULL };
    }

    int oldn = cur->h.nkeys;
    for (int i = 0; i < slot; ++i) tmpk[i] = cur->in.keys[i];
    tmpk[slot] = ch.sep;
    for (int i = slot; i < oldn; ++i) tmpk[i + 1] = cur->in.keys[i];

    for (int i = 0; i < slot; ++i) {
        tmpc[i] = cur->in.child[i];
        node_incref(tmpc[i]);
    }
    tmpc[slot] = ch.node;
    tmpc[slot + 1] = ch.right;
    for (int i = slot + 1; i <= oldn; ++i) {
        tmpc[i + 1] = cur->in.child[i];
        node_incref(tmpc[i + 1]);
    }

    if (oldn < INTERNAL_MAX) {
        Node *n = alloc_node(t, false, gen);
        n->h.nkeys = oldn + 1;
        for (int i = 0; i < oldn + 1; ++i) n->in.keys[i] = tmpk[i];
        for (int i = 0; i <= oldn + 1; ++i) n->in.child[i] = tmpc[i];
        persist_node(t, n);
        t->logical_cow_bytes += PAGE_SIZE_BYTES;
        return (InsertRet){ .node = n, .split = false, .sep = 0, .right = NULL };
    }

    int mid = (INTERNAL_MAX + 1) / 2;
    Key up = tmpk[mid];
    Node *left = alloc_node(t, false, gen);
    Node *right = alloc_node(t, false, gen);

    left->h.nkeys = mid;
    for (int i = 0; i < mid; ++i) left->in.keys[i] = tmpk[i];
    for (int i = 0; i <= mid; ++i) left->in.child[i] = tmpc[i];

    right->h.nkeys = (INTERNAL_MAX + 1) - mid - 1;
    for (int i = 0; i < (int)right->h.nkeys; ++i) right->in.keys[i] = tmpk[mid + 1 + i];
    for (int i = 0; i <= (int)right->h.nkeys; ++i) right->in.child[i] = tmpc[mid + 1 + i];

    persist_node(t, left);
    persist_node(t, right);
    t->logical_cow_bytes += 2ULL * PAGE_SIZE_BYTES;
    return (InsertRet){ .node = left, .split = true, .sep = up, .right = right };
}

static void tree_init(Tree *t, ZnsDevice *zd, uint32_t shard_id) {
    memset(t, 0, sizeof(*t));
    pthread_mutex_init(&t->mu, NULL);
    t->zdev = zd;
    t->shard_id = shard_id;
    t->next_gen = 1;
    t->page_buf = xaligned_alloc(zd->physical_block_size, PAGE_SIZE_BYTES);
    tree_acquire_initial_zone(t);
    t->root = alloc_node(t, true, t->next_gen++);
    persist_node(t, t->root);
}

static void tree_insert_locked(Tree *t, Key k, Val v) {
    pthread_mutex_lock(&t->mu);
    uint64_t gen = t->next_gen++;
    Node *old_root = t->root;
    InsertRet r = insert_rec(t, t->root, k, v, gen);
    if (!r.split) {
        t->root = r.node;
        node_decref(t, old_root);
        t->inserts++;
        pthread_mutex_unlock(&t->mu);
        return;
    }

    Node *new_root = alloc_node(t, false, gen);
    new_root->h.nkeys = 1;
    new_root->in.keys[0] = r.sep;
    new_root->in.child[0] = r.node;
    new_root->in.child[1] = r.right;
    persist_node(t, new_root);
    t->logical_cow_bytes += PAGE_SIZE_BYTES;
    t->root = new_root;
    node_decref(t, old_root);
    t->inserts++;
    pthread_mutex_unlock(&t->mu);
}

static bool tree_lookup_locked(Tree *t, Key k, Val *out) {
    pthread_mutex_lock(&t->mu);
    const Node *n = t->root;
    while (!n->h.is_leaf) {
        int slot = child_slot_internal(n, k);
        n = n->in.child[slot];
    }
    int pos = lower_bound_leaf(n, k);
    bool found = false;
    if (pos < n->h.nkeys && n->lf.keys[pos] == k) {
        if (out) *out = n->lf.vals[pos];
        found = true;
    }
    pthread_mutex_unlock(&t->mu);
    return found;
}

static void index_init(Index *idx, uint32_t nshards, ZnsDevice *zd) {
    assert(nshards > 0);
    idx->nshards = nshards;
    idx->zdev = zd;
    idx->shards = (Tree *)calloc(nshards, sizeof(Tree));
    if (!idx->shards) die_errno("calloc shards");
    for (uint32_t i = 0; i < nshards; ++i) tree_init(&idx->shards[i], zd, i);
}

static inline uint32_t shard_of(const Index *idx, Key k) {
    uint64_t h = mix64(k);
    return (uint32_t)(h % idx->nshards);
}

static void index_insert(Index *idx, Key k, Val v) {
    Tree *t = &idx->shards[shard_of(idx, k)];
    tree_insert_locked(t, k, v);
}

static bool index_lookup(Index *idx, Key k, Val *out) {
    Tree *t = &idx->shards[shard_of(idx, k)];
    return tree_lookup_locked(t, k, out);
}

static void *worker_main(void *arg_) {
    WorkerArg *arg = (WorkerArg *)arg_;
    uint64_t s = arg->seed ^ mix64(arg->tid + 1);
    for (uint64_t i = 0; i < arg->nops; ++i) {
        Key k = xorshift64(&s);
        index_insert(arg->idx, k, i ^ arg->tid);
    }
    return NULL;
}

static uint64_t count_nodes(const Node *n) {
    if (!n) return 0;
    if (n->h.is_leaf) return 1;
    uint64_t c = 1;
    for (uint32_t i = 0; i <= n->h.nkeys; ++i) c += count_nodes(n->in.child[i]);
    return c;
}

static uint64_t tree_height(const Node *n) {
    uint64_t h = 1;
    while (n && !n->h.is_leaf) {
        h++;
        n = n->in.child[0];
    }
    return h;
}

static void print_stats(Index *idx, uint64_t total_ops, double elapsed) {
    uint64_t allocated_nodes = 0;
    uint64_t freed_nodes = 0;
    uint64_t logical_cow_bytes = 0;
    uint64_t device_bytes_written = 0;
    uint64_t reachable_nodes = 0;
    uint64_t persisted_pages = 0;
    uint64_t checkpoint_pages = 0;
    uint64_t max_height = 0;
    uint32_t zones_used = 0;

    for (uint32_t i = 0; i < idx->nshards; ++i) {
        Tree *t = &idx->shards[i];
        allocated_nodes += t->allocated_nodes;
        freed_nodes += t->freed_nodes;
        logical_cow_bytes += t->logical_cow_bytes;
        device_bytes_written += t->device_bytes_written;
        persisted_pages += t->persisted_pages;
        checkpoint_pages += t->checkpoint_pages;
        reachable_nodes += count_nodes(t->root);
        uint64_t h = tree_height(t->root);
        if (h > max_height) max_height = h;
    }
    for (uint32_t i = 0; i < idx->zdev->nr_zones; ++i) {
        if (idx->zdev->zones[i].in_use) zones_used++;
    }

    printf("elapsed=%.6f sec\n", elapsed);
    printf("throughput=%.3f Kops/sec\n", (double)total_ops / elapsed / 1e3);
    printf("shards=%u\n", idx->nshards);
    printf("max_height=%" PRIu64 "\n", max_height);
    printf("reachable_nodes=%" PRIu64 "\n", reachable_nodes);
    printf("allocated_nodes=%" PRIu64 "\n", allocated_nodes);
    printf("freed_nodes=%" PRIu64 "\n", freed_nodes);
    printf("logical_cow_bytes=%" PRIu64 " (%.3f bytes/op)\n",
           logical_cow_bytes, (double)logical_cow_bytes / (double)total_ops);
    printf("device_bytes_written=%" PRIu64 " (%.3f bytes/op)\n",
           device_bytes_written, (double)device_bytes_written / (double)total_ops);
    printf("persisted_pages=%" PRIu64 "\n", persisted_pages);
    printf("checkpoint_pages=%" PRIu64 "\n", checkpoint_pages);
    printf("zones_used=%u / %u sequential zones\n", zones_used, idx->zdev->nr_seq_zones);
}

static void usage(const char *prog) {
    fprintf(stderr,
            "usage: %s <threads> <ops_per_thread> <shards> <seed> [device_path]\n"
            "example: %s 1 10000000 14 1234 /dev/nvme3n2\n"
            "         %s 4 2500000  14 1234 /dev/nvme3n2\n"
            "         %s 16 625000  14 1234 /dev/nvme3n2\n",
            prog, prog, prog, prog);
}

int main(int argc, char **argv) {
    if (argc != 5 && argc != 6) {
        usage(argv[0]);
        return 1;
    }

    uint32_t nthreads = (uint32_t)strtoul(argv[1], NULL, 10);
    uint64_t ops_per_thread = strtoull(argv[2], NULL, 10);
    uint32_t nshards = (uint32_t)strtoul(argv[3], NULL, 10);
    uint64_t seed = strtoull(argv[4], NULL, 10);
    const char *device = (argc == 6) ? argv[5] : DEFAULT_DEVICE;

    if (nthreads == 0 || ops_per_thread == 0 || nshards == 0) {
        usage(argv[0]);
        return 1;
    }

    ZnsDevice zd;
    zns_init(&zd, device);
    if (zd.nr_seq_zones < nshards) {
        fprintf(stderr, "Device %s has only %u sequential zones, but %u shards requested\n",
                device, zd.nr_seq_zones, nshards);
        return 1;
    }

    printf("device=%s\n", device);
    printf("logical_block_size=%u\n", zd.logical_block_size);
    printf("physical_block_size=%u\n", zd.physical_block_size);
    printf("zone_size_bytes=%" PRIu64 "\n", zd.zone_size_bytes);
    printf("zone_capacity_bytes=%" PRIu64 "\n", zd.zone_capacity_bytes);
    printf("sequential_zones=%u\n", zd.nr_seq_zones);
    printf("max_open_zones=%u\n", zd.max_open_zones);
    printf("max_active_zones=%u\n", zd.max_active_zones);

    if (zd.max_active_zones && nshards > zd.max_active_zones) {
        fprintf(stderr,
                "Requested %u shards, but device max_active_zones is %u. "
                "With one zone per shard, the %uth distinct shard write will fail with EOVERFLOW.\n",
                nshards, zd.max_active_zones, zd.max_active_zones + 1);
        return 1;
    }

    Index idx;
    index_init(&idx, nshards, &zd);

    pthread_t *ths = (pthread_t *)calloc(nthreads, sizeof(pthread_t));
    WorkerArg *args = (WorkerArg *)calloc(nthreads, sizeof(WorkerArg));
    if (!ths || !args) die_errno("calloc workers");

    double t0 = now_sec();
    for (uint32_t i = 0; i < nthreads; ++i) {
        args[i].idx = &idx;
        args[i].seed = seed + i * 0x9e3779b97f4a7c15ULL;
        args[i].nops = ops_per_thread;
        args[i].tid = i;
        int rc = pthread_create(&ths[i], NULL, worker_main, &args[i]);
        if (rc != 0) {
            fprintf(stderr, "pthread_create failed: %d\n", rc);
            return 1;
        }
    }
    for (uint32_t i = 0; i < nthreads; ++i) pthread_join(ths[i], NULL);
    double t1 = now_sec();

    uint64_t total_ops = (uint64_t)nthreads * ops_per_thread;
    for (uint32_t i = 0; i < nshards; ++i) {
        persist_checkpoint(&idx.shards[i], total_ops);
    }
    fsync(zd.fd);

    printf("threads=%u\n", nthreads);
    printf("ops_per_thread=%" PRIu64 "\n", ops_per_thread);
    printf("total_ops=%" PRIu64 "\n", total_ops);
    print_stats(&idx, total_ops, t1 - t0);

    uint64_t probe = seed ^ 0x123456789abcdef0ULL;
    uint64_t hits = 0;
    for (int i = 0; i < 10000; ++i) {
        Key k = xorshift64(&probe);
        Val v;
        if (index_lookup(&idx, k, &v)) hits++;
    }
    printf("random_lookup_hits=%" PRIu64 " / 10000\n", hits);

    free(args);
    free(ths);
    zns_destroy(&zd);
    return 0;
}
