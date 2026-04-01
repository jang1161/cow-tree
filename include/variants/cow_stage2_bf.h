#pragma once

#include "cow_zfs.h"

/*
 * Batched flush mechanism for ZFS variant.
 * Replaces the recursive flush_overlay_node with a two-phase approach:
 * Phase 1: collect all dirty overlay nodes via post-order DFS
 * Phase 2: batch-append to NVMe in up to 64-page chunks
 *
 * This reduces system call overhead and improves NVMe utilization.
 */

/*
 * Flush all dirty overlay nodes as a single batched NVMe append.
 * Returns the new root pagenum.
 * Must be called with exclusive access to the overlay (no concurrent writers).
 */
pagenum_t flush_overlay_batched(overlay_state *ov, node_id_t root_id);
