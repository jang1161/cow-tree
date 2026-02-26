#include "file_manager.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <memory.h>


/**
 * @brief Open a database file or create a new one if it doesn't exist.
 * @param file_path[in] The path to the database file.
 * @param leaf_order[in] The leaf order of the B+ tree.
 * @param internal_order[in] The internal order of the B+ tree.
 * @return The file descriptor of the database file. Return -1 if failed.
 *
 * Set the header page and free pages pool during tree creation. Plus, use -1
 * to represent a NULL page number for fields such as the initial root page number
 * in header page or next page number of the last free page.
 */
int open_or_create_tree(const char *file_path, int leaf_order, int internal_order) {
	int fd = open(file_path, O_RDWR);

	if(fd == -1) { // need to make new tree
		if(leaf_order < MIN_LEAF_ORDER || leaf_order > MAX_LEAF_ORDER ||
		internal_order < MIN_INTERNAL_ORDER || internal_order > MAX_INTERNAL_ORDER) {
			printf("Input appropriate leaf/internal order value\n");
			return -1;
		}
		if((fd = open(file_path, O_RDWR | O_CREAT, 0644)) == -1)
			return -1;

		header_page header;
		memset(&header, 0, PAGE_SIZE);
		header.free_pn = 1;
		header.root_pn = -1;
		header.num_pages = INIT_PAGE_COUNT;
		header.leaf_order = leaf_order;
		header.internal_order = internal_order;
		write_header_page(fd, &header);

		page free;
		memset(&free, 0, PAGE_SIZE);
		for(int i = 1; i < INIT_PAGE_COUNT; i++) {
			free.my_pn = i;
			free.parent_pn = (i == INIT_PAGE_COUNT - 1) ? -1 : i + 1; // next free page num
			write_page(fd, &free);
		}
	}
	
	// Disable OS page cache for no-buffer version
	#ifdef __APPLE__
	if(fcntl(fd, F_NOCACHE, 1) == -1) {
		perror("Warning: F_NOCACHE failed");
	}
	#endif

	return fd;
}

/**
 * @brief Load the header page from the database file.
 * @param fd[in] The file descriptor of the database file.
 * @param dest[out] The destination to store the header page.
 */
void load_header_page(int fd, header_page* dest) {
	if(pread(fd, dest, PAGE_SIZE, 0) != PAGE_SIZE)
		exit_with_err_msg("load_header_page error\n");
}

/**
 * @brief Write the header page to the database file.
 * @param fd[in] The file descriptor of the database file.
 * @param src[in] The header page to write.
 */
void write_header_page(int fd, const header_page* src) {
	if(pwrite(fd, src, PAGE_SIZE, 0) != PAGE_SIZE)
		exit_with_err_msg("write_header_page error\n");
}

/**
 * @brief Load a page from the database file.
 * @param fd[in] The file descriptor of the database file.
 * @param pgn[in] The page number of the page to load.
 * @param dest[out] The destination to store the page.
 */
void load_page(int fd, pagenum_t pgn, page* dest) {
	if(pread(fd, dest, PAGE_SIZE, PAGE_SIZE * pgn) != PAGE_SIZE)
		exit_with_err_msg("load_page error\n");
}

/**
 * @brief Write a page to the database file.
 * @param fd[in] The file descriptor of the database file.
 * @param pgn[in] The page number of the page to write.
 * @param src[in] The page to write. Return NULL if failed.
 */
void write_page(int fd, const page* src) {
	if(pwrite(fd, src, PAGE_SIZE, PAGE_SIZE * src->my_pn) != PAGE_SIZE)
		exit_with_err_msg("write_page error\n");
}

/**
 * @brief Allocate a new page.
 * @param fd[in] The file descriptor of the database file.
 * @return The new page.
 *
 * If no free pages are left, this function doubles the free pages pool, and then allocates one.
 * If memory allocation fails, then kill the process using the `exit_with_err_msg()` function.
 */
page *alloc_page(int fd) {
	header_page header;
	load_header_page(fd, &header);

	return alloc_page1(fd, &header);
}

/**
 * @brief Allocate a new page. This function uses the passed header_page pointer instead of calling load_header_page.
 * @param fd[in] The file descriptor of the database file.
 * @param header_page[in] The header page.
 * @return The new page.
 *
 * If no free pages are left, this function doubles the free pages pool, and then allocates one.
 * If memory allocation fails, then kill the process using the `exit_with_err_msg()` function.
 */
page *alloc_page1(int fd, header_page *header_page) {
	if(header_page->free_pn == -1) {
		uint64_t current = header_page->num_pages;
		uint64_t doubled = current * 2;
		page p;
		memset(&p, 0, PAGE_SIZE);
	
		for(uint64_t i = current; i < doubled; i++) {
			p.my_pn = i;
			p.parent_pn = (i == doubled - 1) ? -1 : i + 1;
			if(pwrite(fd, &p, PAGE_SIZE, PAGE_SIZE * i) != PAGE_SIZE)
				exit_with_err_msg("alloc_page1 error\n");
		}
	
		header_page->free_pn = current;
		header_page->num_pages = doubled;
		write_header_page(fd, header_page);
	}

	page *p_new = (page *)malloc(PAGE_SIZE);
	if(!p_new)
		exit_with_err_msg("alloc_page1 error\n");
	load_page(fd, header_page->free_pn, p_new);

	header_page->free_pn = p_new->parent_pn;
	write_header_page(fd, header_page);

	pagenum_t allocated_pn = p_new->my_pn;
	memset(p_new, 0, PAGE_SIZE);
	p_new->my_pn = allocated_pn;

	return p_new;
}

/**
 * @brief Free a page.
 * @param fd[in] The file descriptor of the database file.
 * @param pgn[in] The page number of the page to free.
 */
void free_page(int fd, pagenum_t pgn) {
    header_page header;
	page freed;
	load_header_page(fd, &header);
	
	memset(&freed, 0, PAGE_SIZE);
	freed.parent_pn = header.free_pn;
	freed.my_pn = pgn;
	header.free_pn = pgn;

	write_header_page(fd, &header);
	write_page(fd, &freed);
}

/**
 * @brief Print an error message and exit the program.
 * @param err_msg[in] The error message to print.
 */
void exit_with_err_msg(const char* err_msg) {
	perror(err_msg);
	exit(EXIT_FAILURE);
}
