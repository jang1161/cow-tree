#define _GNU_SOURCE

#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "cow_zfs.h"
#include "cow_stage2_bf.h"

/*
 * Get current monotonic time in nanoseconds.
 */
static inline uint64_t monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/*
 * Configuration for batched append.
 */
#define MAX_BATCH_PAGES 2048
#define MAX_NVME_PAGES  64

/*
 * Entry for a node to be flushed.
 */
typedef struct
{
    overlay_node *n;
} batch_entry;

/*
 * Inline helper to check if id is a temp node.
 */
static inline int is_temp_id(node_id_t id)
{
    return (id & (1ULL << 63)) != 0;
}

/*
 * Inline helper to rotate to the next zone.
 */
static inline uint32_t zone_next(cow_tree *t, uint32_t zone_id)
{
    return (zone_id + 1 < t->info.nr_zones) ? (zone_id + 1) : t->info.nr_zones;
}

/*
 * Hash function for overlay node IDs.
 */
static inline uint64_t overlay_hash_u64(uint64_t x)
{
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 33;
    return x;
}

/*
 * Find index of node in overlay by id.
 * Returns -1 if not found.
 */
static int overlay_find_idx(overlay_state *ov, node_id_t id)
{
    if (ov->idx_cap == 0)
        return -1;

    size_t m = ov->idx_cap - 1;
    size_t pos = (size_t)(overlay_hash_u64((uint64_t)id) & m);
    for (;;)
    {
        size_t v = ov->idx_table[pos];
        if (v == (size_t)-1)
            return -1;
        if (ov->arr[v].id == id)
            return (int)v;
        pos = (pos + 1) & m;
    }
}

/*
 * Reserve a writable zone, rotating if needed.
 */
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

/*
 * Perform a zone append of N pages in a single NVMe command.
 */
static int zone_append_n_pages(cow_tree *t, uint32_t zone_id, const void *buf,
                               int n_pages, pagenum_t *out_pn, uint64_t *out_wp_bytes);

/*
 * Resolve a child node_id to its final on-disk pagenum.
 * Called after Phase 1 collection, when flushed_pn is already set for all
 * nodes in the overlay.
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
 * new pagenum (COW propagation). Children are always processed before their
 * parent, guaranteeing that when we build a parent page its children's
 * flushed_pn values are already final.
 *
 * Nodes that do not need rewriting are marked flushed with flushed_pn == id
 * (their existing on-disk pagenum). Nodes that do need rewriting are added
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
        if (*count >= max)
        {
            fprintf(stderr, "collect_dirty_nodes: exceeded MAX_BATCH_PAGES (%d)\n", max);
            exit(EXIT_FAILURE);
        }
        n->flushed = 1;
        n->flushed_pn = (pagenum_t)(*count); /* temp: will become base_pn + idx */
        entries[*count].n = n;
        (*count)++;
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
 * Forward declarations of external functions from cow_zfs.c.
 * These must be made non-static in cow_zfs.c for linking.
 */
extern void ram_table_insert(cow_tree *t, pagenum_t pn, const page *src);
extern void global_cache_insert(pagenum_t pn, const page *src);
extern int zone_append_raw_nolock(cow_tree *t, uint32_t zone_id, const void *buf,
                                  pagenum_t *out_pn, uint64_t *out_wp_bytes);

/*
 * Append N pages in a single NVMe ZNS append command.
 * Returns 0 on success, -1 on failure.
 */
static int zone_append_n_pages(cow_tree *t, uint32_t zone_id, const void *buf,
                               int n_pages, pagenum_t *out_pn, uint64_t *out_wp_bytes)
{
    uint32_t total_bytes = (uint32_t)n_pages * PAGE_SIZE;
    __u16    nlb         = (__u16)((total_bytes / t->info.lblock_size) - 1);
    __u64    zslba       = t->zones[zone_id].start / t->info.lblock_size;
    __u64    result      = 0;

    struct nvme_zns_append_args args = {
        .zslba        = zslba,
        .result       = &result,
        .data         = (void *)buf,
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
        return -1;

    if (out_wp_bytes)
        *out_wp_bytes = (result + (__u64)nlb + 1) * t->info.lblock_size;
    if (out_pn)
        *out_pn = (pagenum_t)(result * t->info.lblock_size / PAGE_SIZE);

    return 0;
}

/*
 * Flush all dirty overlay nodes as a single (chunked) NVMe ZNS append.
 *
 * Algorithm:
 *   1. Post-order DFS to collect dirty nodes; flushed_pn = batch index.
 *   2. Pick a zone that has enough space for count pages; predict base_pn
 *      from the software write-pointer.
 *   3. Assign real pagenums: flushed_pn = base_pn + index.
 *   4. Build page data (internal nodes resolve child pointers via flushed_pn).
 *   5. Append all pages in MAX_NVME_PAGES-sized NVMe commands.
 *   6. Insert every page into the RAM cache and global page cache.
 */
pagenum_t flush_overlay_batched(overlay_state *ov, node_id_t root_id)
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
        zone_id = reserve_writable_zone(t);

        base_wp = atomic_load_explicit(&t->zone_wp_bytes[zone_id], memory_order_acquire);
        uint64_t zone_end = t->zones[zone_id].start + t->zones[zone_id].capacity;
        uint64_t needed   = (uint64_t)count * PAGE_SIZE;

        if (base_wp + needed > zone_end)
        {
            /* Doesn't fit — mark full and rotate */
            atomic_store_explicit(&t->zone_full[zone_id], 1, memory_order_release);
            uint32_t expected = zone_id;
            uint32_t next = zone_next(t, zone_id);
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, next,
                memory_order_acq_rel, memory_order_acquire);
            continue;
        }
        break;
    }

    /*
     * Predict base_pn from the software write-pointer.
     */
    pagenum_t base_pn = (pagenum_t)(base_wp / PAGE_SIZE);

    /* ------------------------------------------------------------------ */
    /* Update software WP for all count pages atomically                   */
    /* ------------------------------------------------------------------ */
    uint64_t total_bytes = (uint64_t)count * PAGE_SIZE;
    uint64_t old_wp = atomic_fetch_add_explicit(
        &t->zone_wp_bytes[zone_id], total_bytes, memory_order_acq_rel);

    uint64_t zone_end = t->zones[zone_id].start + t->zones[zone_id].capacity;
    if (old_wp + total_bytes > zone_end)
    {
        fprintf(stderr, "batch flush: zone space check failed unexpectedly\n");
        exit(EXIT_FAILURE);
    }

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
    uint64_t t0 = monotonic_ns();
    int done = 0;
    pagenum_t actual_base = 0;

    while (done < count)
    {
        int chunk = count - done;
        if (chunk > MAX_NVME_PAGES)
            chunk = MAX_NVME_PAGES;

        char    *chunk_buf = (char *)buf + (size_t)done * PAGE_SIZE;
        uint64_t wp_bytes  = 0;
        pagenum_t chunk_pn = 0;

        if (zone_append_n_pages(t, zone_id, chunk_buf, chunk, &chunk_pn, &wp_bytes) != 0)
        {
            perror("zone_append_n_pages (batch)");
            exit(EXIT_FAILURE);
        }

        if (done == 0)
            actual_base = chunk_pn;

        /* Update zone WP from hardware result */
        uint64_t cur_wp = atomic_load_explicit(&t->zone_wp_bytes[zone_id], memory_order_acquire);
        while (wp_bytes > cur_wp &&
               !atomic_compare_exchange_weak_explicit(
                   &t->zone_wp_bytes[zone_id], &cur_wp, wp_bytes,
                   memory_order_acq_rel, memory_order_acquire))
        {
        }

        done += chunk;
    }
    uint64_t append_dt = monotonic_ns() - t0;

    /*
     * If the hardware placed pages at a different base than predicted,
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
    /* Insert all pages into both RAM table and global cache               */
    /* ------------------------------------------------------------------ */
    for (int i = 0; i < count; i++)
    {
        overlay_node *n  = entries[i].n;
        page         *pg = (page *)((char *)buf + (size_t)i * PAGE_SIZE);
        ram_table_insert(t, n->flushed_pn, pg);
        global_cache_insert(n->flushed_pn, pg);
    }

    atomic_fetch_add_explicit(&t->stat_append_ns_sum, append_dt, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_append_ns_samples, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&t->stat_page_appends, (uint64_t)count, memory_order_relaxed);

    /* Check / update zone-full state after the append */
    {
        uint64_t cur_wp = atomic_load_explicit(&t->zone_wp_bytes[zone_id], memory_order_acquire);
        uint64_t zone_end_chk = t->zones[zone_id].start + t->zones[zone_id].capacity;
        if (cur_wp >= zone_end_chk)
        {
            atomic_store_explicit(&t->zone_full[zone_id], 1, memory_order_release);
            uint32_t expected = zone_id;
            uint32_t next = zone_next(t, zone_id);
            atomic_compare_exchange_weak_explicit(
                &t->current_zone, &expected, next,
                memory_order_acq_rel, memory_order_acquire);
        }
    }

    free(buf);

    return (root_idx >= 0) ? ov->arr[root_idx].flushed_pn : (pagenum_t)root_id;
}
