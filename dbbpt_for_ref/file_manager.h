#ifndef __FILE_MANAGER_H__
#define __FILE_MANAGER_H__

#include <stdint.h>
#include <stdbool.h>
#ifdef _WIN32
#define bool char
#define false 0
#define true 1
#endif

#define MIN_LEAF_ORDER 3
#define MIN_INTERNAL_ORDER 3
#define MAX_LEAF_ORDER 32
#define MAX_INTERNAL_ORDER 249
#define DEFAULT_LEAF_ORDER 32
#define DEFAULT_INTERNAL_ORDER 249

#define INIT_PAGE_COUNT 4

// Type definitions
typedef uint64_t pagenum_t;

// Constants
#define PAGE_SIZE 4096
#define HEADER_PAGE_NUM 0

// Structures
typedef struct record {
	char value[120];
} record;

typedef struct leaf_entity {
	uint64_t key;
	record record;
} leaf_entity;

typedef struct internal_entity {
	uint64_t key;
	pagenum_t child;
} internal_entity;

typedef struct page {
	pagenum_t my_pn;
	pagenum_t parent_pn; 	// free page: next free page num, root page: 0
	uint32_t is_leaf;
	uint32_t num_keys;
	pagenum_t pointer;		// internal: right most child, leaf: right sibling
	uint8_t reserved[128 - (
		sizeof(pagenum_t) * 3 +
		sizeof(uint32_t) * 2
	)];						// first 128 bytes for header

	union {
		leaf_entity leaf[31];
		internal_entity internal[248];
	};
} page;

typedef struct header_page {
	pagenum_t free_pn;
	pagenum_t root_pn;
	uint64_t num_pages;
	uint32_t leaf_order;
	uint32_t internal_order;

	uint8_t reserved[PAGE_SIZE - (
		sizeof(pagenum_t) * 2 +
		sizeof(uint64_t) +
		sizeof(uint32_t) * 2
	)];
} header_page;

// Function prototypes
int open_or_create_tree(const char *file_path, int leaf_order, int internal_order);
void load_header_page(int fd, header_page* dest);
void write_header_page(int fd, const header_page* src);
void load_page(int fd, pagenum_t pgn, page* dest);
void write_page(int fd, const page* src);
page *alloc_page(int fd);
page *alloc_page1(int fd, header_page* header);
void free_page(int fd, pagenum_t pgn);

// Helper functions
void exit_with_err_msg(const char* err_msg);

#endif /* __FILE_MANAGER_H__ */
