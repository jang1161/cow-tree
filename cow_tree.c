#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include "cow_tree.h"

pagenum_t cow_zone_base(cow_tree *t, uint32_t zone_id) {
    return (pagenum_t)zone_id * (t->info.zone_size / PAGE_SIZE);
}

uint32_t cow_pn_to_zone(cow_tree *t, pagenum_t pn) {
    return (uint32_t)(pn / (t->info.zone_size / PAGE_SIZE));
}

static void load_page(cow_tree *t, pagenum_t pn, page *dst) {
    if (pread(t->fd, dst, PAGE_SIZE, (off_t)pn * PAGE_SIZE) != PAGE_SIZE)
        perror("load_page");
        exit(EXIT_FAILURE);
}

static pagenum_t zone_append_raw(cow_tree *t, uint32_t zone_id, const void *buf) {
    struct zbd_zone *zn = &t->zones[zone_id];
    __u64 zslba = zn->start / t->info.lblock_size;
    __u16 nlb = (PAGE_SIZE / t->info.lblock_size) - 1; // 0-based
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
        .ilbrt_u64 = 0
    };

    if (nvme_zns_append(&args) != 0) {
        perror("nvme_zns_append");
        exit(EXIT_FAILURE);
    }

    zn->wp = (result + nlb + 1) * t->info.lblock_size;
    zn->cond = (zn->wp >= zn->start + zn->capacity)
               ? ZBD_ZONE_COND_FULL : ZBD_ZONE_COND_IMP_OPEN;

    return (pagenum_t)(result * t->info.lblock_size / PAGE_SIZE);
}

static pagenum_t cow_append_page(cow_tree *t, page *p) {
    if (t->zones[t->current_zone].cond == ZBD_ZONE_COND_FULL) {
        t->current_zone++;
        if (t->current_zone >= t->info.nr_zones) {
            fprintf(stderr, "zones exhausted — benchmark complete\n");
            exit(EXIT_SUCCESS);
        }
    }
    pagenum_t pn = zone_append_raw(t, t->current_zone, p);
    p->pn = pn;
    return pn;
}

static uint64_t scan_meta_zone(int fd, uint32_t zone_id, uint64_t zone_size, superblock_entry *out) {
    uint64_t zone_pages = zone_size / PAGE_SIZE;
    uint64_t last_wp = 0;
    for (uint64_t i = 1; i < zone_pages; i++) {
        superblock_entry tmp;
        off_t off = (off_t)zone_id * zone_size + (off_t)i * PAGE_SIZE;
        if (pread(fd, &tmp, PAGE_SIZE, off) != PAGE_SIZE) break;
        if (tmp.magic != SB_MAGIC) break;
        *out = tmp;
        last_wp = i;
    }
    return last_wp;
}

static void activate_meta_zone(cow_tree *t, uint32_t zone_id, uint64_t version) {
    off_t zstart = (off_t)zone_id * t->info.zone_size;
    if (zbd_reset_zones(t->fd, zstart, (off_t)t->info.zone_size) != 0) {
        perror("zbd_reset_zones"); exit(EXIT_FAILURE);
    }
    t->zones[zone_id].wp   = t->zones[zone_id].start;
    t->zones[zone_id].cond = ZBD_ZONE_COND_EMPTY;

    zone_header zh;
    memset(&zh, 0, sizeof zh);
    zh.magic      = ZH_MAGIC;
    zh.state      = ZH_ACTIVE;
    zh.version = version;
    zone_append_raw(t, zone_id, &zh);

    t->active_zone = zone_id;
    t->meta_wp = 1; // page 0 for zone header
    t->version = version;
}

static void load_superblock(cow_tree *t)
{
    zone_header zh0, zh1;
    int v0 = (pread(t->fd, &zh0, PAGE_SIZE, 0) == PAGE_SIZE) && (zh0.magic == ZH_MAGIC);
    int v1 = (pread(t->fd, &zh1, PAGE_SIZE, (off_t)META_ZONE_1 * t->info.zone_size) == PAGE_SIZE)
             && (zh1.magic == ZH_MAGIC);

    superblock_entry sb0, sb1;
    uint64_t wp0 = 0, wp1 = 0;
    if (v0) wp0 = scan_meta_zone(t->fd, META_ZONE_0, t->info.zone_size, &sb0);
    if (v1) wp1 = scan_meta_zone(t->fd, META_ZONE_1, t->info.zone_size, &sb1);

    if (wp0 == 0 && wp1 == 0) {
        memset(&t->sb, 0, sizeof t->sb);
        t->sb.root_pn        = INVALID_PGN;
        t->sb.leaf_order     = LEAF_ORDER;
        t->sb.internal_order = INTERNAL_ORDER;
        activate_meta_zone(t, META_ZONE_0, 0);
        return;
    }

    if (wp0 > 0 && wp1 > 0) {
        if (sb0.seq_no >= sb1.seq_no) { t->sb = sb0; t->active_zone = META_ZONE_0; t->meta_wp = wp0 + 1; t->version = zh0.version; }
        else                          { t->sb = sb1; t->active_zone = META_ZONE_1; t->meta_wp = wp1 + 1; t->version = zh1.version; }
    } else if (wp0 > 0) {
        t->sb = sb0; t->active_zone = META_ZONE_0; t->meta_wp = wp0 + 1; t->version = zh0.version;
    } else {
        t->sb = sb1; t->active_zone = META_ZONE_1; t->meta_wp = wp1 + 1; t->version = zh1.version;
    }
}

static void write_superblock(cow_tree *t) {
    t->sb.magic = SB_MAGIC;
    t->sb.seq_no++;

    /* Zone switch: active meta zone is full */
    if (t->meta_wp >= t->info.zone_size / PAGE_SIZE) {
        uint32_t new_zone = 1 - t->active_zone;
        /*
         * activate_meta_zone issues ZONE RESET on new_zone and writes the
         * zone_header via Zone Append, leaving meta_wp = 1.
         */
        activate_meta_zone(t, new_zone, t->version + 1);
    }

    zone_append_raw(t, t->active_zone, &t->sb);
    t->meta_wp++;
}

cow_tree *cow_open(const char *path) {
    cow_tree *t = malloc(sizeof *t);
    if (!t) { perror("malloc"); return NULL; }

    t->fd = zbd_open(path, O_RDWR, &t->info);
    if (t->fd < 0) { perror("zbd_open"); free(t); return NULL; }

    if (nvme_get_nsid(t->fd, &t->nsid) != 0) {
        perror("nvme_get_nsid"); zbd_close(t->fd); free(t); return NULL;
    }

    /* Read zone metadata (WP, capacity, cond) for all zones from the device. */
    t->zones = calloc(t->info.nr_zones, sizeof *t->zones);
    if (!t->zones) { perror("calloc"); zbd_close(t->fd); free(t); return NULL; }

    unsigned int nr = t->info.nr_zones;
    if (zbd_report_zones(t->fd, 0, 0, ZBD_RO_ALL, t->zones, &nr) != 0) {
        perror("zbd_report_zones");
        free(t->zones); zbd_close(t->fd); free(t); return NULL;
    }

    load_superblock(t);

    /* Restore current_zone: first data zone that is not FULL. */
    t->current_zone = DATA_ZONE_START;
    for (uint32_t z = DATA_ZONE_START; z < t->info.nr_zones; z++) {
        if (t->zones[z].cond != ZBD_ZONE_COND_FULL) {
            t->current_zone = z;
            break;
        }
    }

    return t;
}

void cow_close(cow_tree *t) {
    if (t) {
        free(t->zones);
        zbd_close(t->fd);
        free(t);
    }
}

static int is_empty(cow_tree *t) { return t->sb.root_pn == INVALID_PGN; }

static uint32_t get_position(page *p, int64_t key) {
    if (p->is_leaf) {
        for (uint32_t i = 0; i < p->num_keys; i++)
            if (key < (int64_t)p->leaf[i].key) return i;
    } else {
        for (uint32_t i = 0; i < p->num_keys; i++)
            if (key < (int64_t)p->internal[i].key) return i;
    }
    return p->num_keys;
}

static int leaf_is_full(page *p)     { return p->num_keys == LEAF_ORDER - 1; }
static int internal_is_full(page *p) { return p->num_keys == INTERNAL_ORDER - 1; }

/* ═══════════════════════════════════════════════════════════
 * Traversal path
 * ═══════════════════════════════════════════════════════════
 *
 * path_entry.cidx:
 *   0 .. num_keys-1 → we followed internal[cidx].child
 *   RIGHTMOST_IDX   → we followed .pointer (rightmost child)
 */

#define MAX_HEIGHT    16
#define RIGHTMOST_IDX UINT32_MAX

typedef struct { pagenum_t pn; uint32_t cidx; } path_entry;
typedef struct { path_entry e[MAX_HEIGHT]; int depth; } tpath;

static pagenum_t find_leaf(cow_tree *t, int64_t key, tpath *path) {
    path->depth = 0;
    page p;
    load_page(t, t->sb.root_pn, &p);

    while (!p.is_leaf) {
        uint32_t idx = RIGHTMOST_IDX;
        for (uint32_t i = 0; i < p.num_keys; i++) {
            if (key < (int64_t)p.internal[i].key) { 
                idx = i; 
                break; 
            }
        }
            
        path->e[path->depth++] = (path_entry){ p.pn, idx };
        load_page(t, (idx == RIGHTMOST_IDX) ? p.pointer : p.internal[idx].child, &p);
    }
    path->e[path->depth++] = (path_entry){ p.pn, RIGHTMOST_IDX };
    return p.pn;
}

static void cow_propagate(cow_tree *t, tpath *path, int from_level,
                           pagenum_t new_child,
                           int has_split, int64_t split_key, pagenum_t split_right,
                           int has_sep,   int64_t old_sep,   int64_t new_sep)
{
    pagenum_t cur   = new_child;
    int       split = has_split;
    int64_t   pkey  = split_key;
    pagenum_t pright= split_right;

    for (int i = from_level - 1; i >= 0; i--) {
        page anc;
        load_page(t, path->e[i].pn, &anc);
        uint32_t cidx = path->e[i].cidx;
        uint32_t pos  = (cidx == RIGHTMOST_IDX) ? anc.num_keys : cidx;

        /* Optional: update a changed separator key in this ancestor */
        if (has_sep) {
            for (uint32_t k = 0; k < anc.num_keys; k++) {
                if ((int64_t)anc.internal[k].key == old_sep) {
                    anc.internal[k].key = (uint64_t)new_sep;
                    has_sep = 0;
                    break;
                }
            }
        }

        /* Update child pointer from stale location to cur */
        if (cidx == RIGHTMOST_IDX) anc.pointer              = cur;
        else                       anc.internal[cidx].child = cur;

        if (!split) {
            /* Simple CoW: just the pointer update */
            cur = cow_append_page(t, &anc);

        } else if (!internal_is_full(&anc)) {
            /* Space available: insert (pkey, pright) at pos */
            for (int64_t j = (int64_t)anc.num_keys - 1; j >= (int64_t)pos; j--)
                anc.internal[j+1] = anc.internal[j];
            anc.internal[pos].key   = (uint64_t)pkey;
            anc.internal[pos].child = cur;
            if (pos == anc.num_keys) anc.pointer              = pright;
            else                     anc.internal[pos+1].child= pright;
            anc.num_keys++;
            cur   = cow_append_page(t, &anc);
            split = 0;

        } else {
            /* Full internal node: split it.
             *
             * Build combined key array   tkeys[0..ORDER-1]      (ORDER elements)
             * Build combined child array tchld[0..ORDER]         (ORDER+1 elements)
             * where ORDER = INTERNAL_ORDER.
             */
            const uint32_t ORDER = INTERNAL_ORDER;
            int64_t   *tkeys = malloc(ORDER       * sizeof *tkeys);
            pagenum_t *tchld = malloc((ORDER + 1) * sizeof *tchld);

            for (uint32_t j = 0; j < pos; j++)
                tkeys[j] = (int64_t)anc.internal[j].key;
            tkeys[pos] = pkey;
            for (uint32_t j = pos; j < ORDER - 1; j++)
                tkeys[j+1] = (int64_t)anc.internal[j].key;

            for (uint32_t j = 0; j < pos; j++)
                tchld[j] = anc.internal[j].child;
            tchld[pos]   = cur;
            tchld[pos+1] = pright;
            for (uint32_t j = pos + 1; j < ORDER; j++)
                tchld[j+1] = (j < ORDER - 1) ? anc.internal[j].child : anc.pointer;

            uint32_t sp    = (ORDER + 1) / 2;
            int64_t  newpk = tkeys[sp - 1];

            /* Rebuild left half into anc */
            for (uint32_t j = 0; j < sp - 1; j++) {
                anc.internal[j].key   = (uint64_t)tkeys[j];
                anc.internal[j].child = tchld[j];
            }
            anc.pointer  = tchld[sp - 1];
            anc.num_keys = sp - 1;

            /* Build right half */
            page r;
            memset(&r, 0, sizeof r);
            r.is_leaf   = 0;
            for (uint32_t j = sp; j < ORDER; j++) {
                r.internal[j - sp].key   = (uint64_t)tkeys[j];
                r.internal[j - sp].child = tchld[j];
            }
            r.pointer  = tchld[ORDER];
            r.num_keys = ORDER - sp;

            free(tkeys); free(tchld);

            pagenum_t r_pn = cow_append_page(t, &r);
            cur    = cow_append_page(t, &anc);
            pkey   = newpk;
            pright = r_pn;
            split  = 1;
        }
    }

    if (!split) {
        t->sb.root_pn = cur;
    } else {
        /* Create new root */
        page root;
        memset(&root, 0, sizeof root);
        root.is_leaf           = 0;
        root.num_keys          = 1;
        root.internal[0].key   = (uint64_t)pkey;
        root.internal[0].child = cur;
        root.pointer           = pright;
        t->sb.root_pn = cow_append_page(t, &root);
    }
    write_superblock(t);
}

record *cow_find(cow_tree *t, int64_t key) {
    if (is_empty(t)) return NULL;

    tpath path;
    find_leaf(t, key, &path);

    page leaf;
    load_page(t, path.e[path.depth - 1].pn, &leaf);

    for (uint32_t i = 0; i < leaf.num_keys; i++) {
        if ((int64_t)leaf.leaf[i].key == key) {
            record *r = malloc(sizeof *r);
            *r = leaf.leaf[i].record;
            return r;
        }
    }
    return NULL;
}

void cow_insert(cow_tree *t, int64_t key, const char *value)
{
    /* ── Empty tree: create the first root leaf ── */
    if (is_empty(t)) {
        page root;
        memset(&root, 0, sizeof root);
        root.is_leaf          = 1;
        root.num_keys         = 1;
        root.pointer          = INVALID_PGN;
        root.leaf[0].key      = (uint64_t)key;
        memcpy(root.leaf[0].record.value, value, 120);
        t->sb.root_pn = cow_append_page(t, &root);
        write_superblock(t);
        return;
    }

    tpath path;
    find_leaf(t, key, &path);
    page leaf;
    load_page(t, path.e[path.depth - 1].pn, &leaf);

    if (!leaf_is_full(&leaf)) {
        /* ── Simple insert ── */
        uint32_t pos = get_position(&leaf, key);
        for (int64_t j = (int64_t)leaf.num_keys - 1; j >= (int64_t)pos; j--)
            leaf.leaf[j+1] = leaf.leaf[j];
        leaf.leaf[pos].key = (uint64_t)key;
        memcpy(leaf.leaf[pos].record.value, value, 120);
        leaf.num_keys++;
        pagenum_t new_leaf = cow_append_page(t, &leaf);
        cow_propagate(t, &path, path.depth - 1, new_leaf,
                      0, 0, INVALID_PGN, 0, 0, 0);
    } else {
        /* ── Leaf is full: split ── */
        const uint32_t ORDER = LEAF_ORDER;
        leaf_entity *tmp = malloc(ORDER * sizeof *tmp);

        uint32_t pos = 0;
        while (pos < leaf.num_keys && (int64_t)leaf.leaf[pos].key < key) pos++;

        for (uint32_t i = 0; i < pos; i++)           tmp[i]   = leaf.leaf[i];
        tmp[pos].key = (uint64_t)key;
        memcpy(tmp[pos].record.value, value, 120);
        for (uint32_t i = pos; i < leaf.num_keys; i++) tmp[i+1] = leaf.leaf[i];

        uint32_t sp = ORDER / 2;

        /* Left leaf (reuse struct, new location) */
        for (uint32_t i = 0; i < sp; i++) leaf.leaf[i] = tmp[i];
        leaf.num_keys = sp;

        /* Right leaf */
        page right;
        memset(&right, 0, sizeof right);
        right.is_leaf   = 1;
        for (uint32_t i = 0; i < ORDER - sp; i++) right.leaf[i] = tmp[sp + i];
        right.num_keys = ORDER - sp;
        right.pointer  = leaf.pointer;   /* inherit right-sibling link */

        free(tmp);

        /* Write right first to get its pn, then update left's sibling link */
        pagenum_t right_pn = cow_append_page(t, &right);
        leaf.pointer       = right_pn;
        pagenum_t left_pn  = cow_append_page(t, &leaf);

        int64_t promote = (int64_t)right.leaf[0].key;
        cow_propagate(t, &path, path.depth - 1, left_pn,
                      1, promote, right_pn, 0, 0, 0);
    }
}