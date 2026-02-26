#pragma once
#include <stdint.h>
#include <libzbd/zbd.h>
#include <libnvme.h>
#include <pthread.h>

/* ── ZNS device parameters ── */
#define PAGE_SIZE           4096
#define META_ZONE_0         0
#define META_ZONE_1         1
#define DATA_ZONE_START     2

/* ── Magic numbers ── */
#define ZH_MAGIC            0x5A4E535A48445200ULL // zone header
#define SB_MAGIC            0x434F574250545245ULL // superblock
#define ZH_ACTIVE           0x01

/* ── B+-tree parameters ── */
#define LEAF_ORDER          32
#define INTERNAL_ORDER      249
#define LEAF_MIN            ((LEAF_ORDER - 1) / 2)
#define INTERNAL_MIN        ((INTERNAL_ORDER - 1) / 2)  

typedef uint64_t pagenum_t;
#define INVALID_PGN         ((pagenum_t)-1)

/* ── On-disk structures ── */

typedef struct record {
	char value[120];
} record;

typedef struct leaf_entity {
	uint64_t    key;
	record      record;
} leaf_entity;

typedef struct internal_entity {
	uint64_t    key;
	pagenum_t   child;
} internal_entity;

typedef struct {
    pagenum_t   pn;
    // pagenum_t parent_pn; not used in CoW ver.
    uint32_t    is_leaf;
    uint32_t    num_keys;
    pagenum_t   pointer; // internal: righmost child; leaf: right sibling
    uint8_t     pad[128 - (
        sizeof(pagenum_t) * 2 +
        sizeof(uint32_t) * 2
    )];

    union {
        leaf_entity     leaf[LEAF_ORDER - 1];
        internal_entity internal[INTERNAL_ORDER - 1];
    };
} page;

typedef struct {
    uint64_t    magic;
    uint8_t     state;
    uint64_t    version;
    uint8_t     pad[PAGE_SIZE - 8 - 1 - 8];
} zone_header;

typedef struct {
    uint64_t    magic;
    uint64_t    seq_no;
    pagenum_t   root_pn;
    uint32_t    leaf_order;
    uint32_t    internal_order;
    uint8_t     pad[PAGE_SIZE - 8 * 3 - 4 * 2]
} superblock_entry;

typedef struct {
    int              fd;
    __u32            nsid; // NVMe namespace ID
    struct zbd_info  info;
    struct zbd_zone *zones;
    uint32_t         current_zone; // single activae data zone
    superblock_entry sb;
    uint32_t         active_zone; // Metazone 0 or 1
    uint64_t         meta_wp;
    uint64_t         version;
} cow_tree;

/* ──────────────────── API ──────────────────── */

cow_tree *cow_open(const char *path);
void      cow_close(cow_tree *t);

record   *cow_find(cow_tree *t, int64_t key);
void      cow_insert(cow_tree *t, int64_t key, const char *value);

pagenum_t cow_zone_base(cow_tree *t, uint32_t zone_id);
uint32_t  cow_pn_to_zone(cow_tree *t, pagenum_t pn);
