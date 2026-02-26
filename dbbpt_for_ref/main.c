#include "file_manager.h"
#include "dbbpt.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#ifdef _WIN32
#define bool char
#define false 0
#define true 1
#endif
#include <unistd.h>

// Constant for optional command-line input with "i" command.
#define BUFFER_SIZE 256


// TYPES.
typedef struct int_pair {
	int64_t first;
	int64_t second;
	struct int_pair *next;
} int_pair;

typedef struct  int_pair_queue {
	int_pair *head;
	int_pair *tail;
} int_pair_queue;


// FUNCTION PROTOTYPES.
// Command processing functions
void process_command(char* command_line, bool need_echo, bool need_response, bool need_help);
void process_commands(FILE* stream, bool need_echo, bool need_response);

// Printing tree functions
void print_tree(int fd);
void print_leaves(int fd);
void find_and_print(int fd, int64_t key, bool verbose);

// Utility functions.
int_pair *make_int_pair(int first, int second);
void enqueue(int_pair* new_pair);
int_pair* dequeue(void);

// Common printing functions.
void license_notice(void);
void usage_1(void);
void usage_2(void);


// GLOBALS.
int_pair_queue print_queue = { NULL, NULL };
bool verbose_output = false;
int tree_fd = -1;


// MAIN.
int main(int argc, char ** argv) {
	verbose_output = false;
	tree_fd = -1;

	license_notice();
	usage_1();
	usage_2();

	char buffer[BUFFER_SIZE] = {0};
	while (true) {
		printf("> ");
		if (fgets(buffer, BUFFER_SIZE, stdin) == NULL) break;
		char instruction;
		if (sscanf(buffer, " %c", &instruction) < 1) continue;
		if (instruction == 'q') break;
		if (instruction == 'e') {
			char command_file_path[256] = {0};
			int need_echo_int = 0;
			int need_response_int = 0;
			int count = sscanf(buffer, "e %s %d %d", command_file_path, &need_echo_int, &need_response_int);

			if (count >= 1) {
				FILE* command_fp = fopen(command_file_path, "r");
				if (command_fp == NULL) {
					fprintf(stderr, "Error: Could not open command file '%s'.\n", command_file_path);
				} else {
					bool need_echo = (count < 2) ? false : (need_echo_int == 1);
					bool need_response = (count < 3) ? false : (need_response_int == 1);
					process_commands(command_fp, need_echo, need_response);
					fclose(command_fp);
				}
			}
			continue;
		}
		process_command(buffer, false, true, true);
	}
	printf("\n");

	return EXIT_SUCCESS;
}

// Command processing functions
void process_commands(FILE* stream, bool need_echo, bool need_response) {
	char buffer[BUFFER_SIZE];

	while (true) {
		if (fgets(buffer, BUFFER_SIZE, stream) == NULL) break; // EOF or error

		size_t len = strlen(buffer);
		if (len > 0 && buffer[len - 1] != '\n' && len < BUFFER_SIZE - 1) {
			buffer[len] = '\n';
			buffer[len + 1] = '\0';
		}
		process_command(buffer, need_echo, need_response, false);
	}
}

void process_command(char* command_line, bool need_echo, bool need_response, bool need_help) {
	char instruction;
	if (sscanf(command_line, " %c", &instruction) < 1) return;
	if (instruction == '#') {
		if (strlen(command_line) < 3) printf("\n");
		else printf("%s", command_line + 2);
		return;
	}

	if (need_echo) printf("> %s", command_line);
	if (instruction == 'o') {
		if (tree_fd != -1) {
			if (need_response) printf("A database file is already open. Please close it first with 'c'.\n");
			return;
		}
		char filepath[256] = {0};
		int leaf_order = DEFAULT_LEAF_ORDER;
		int internal_order = DEFAULT_INTERNAL_ORDER;
		int count = sscanf(command_line, "o %s %d %d", filepath, &leaf_order, &internal_order);
		if (count >= 1) {
			tree_fd = open_or_create_tree(filepath, leaf_order, internal_order);
			if (need_response && tree_fd != -1) printf("File '%s' opened.\n", filepath);
		} else if (need_help) {
			usage_2();
		}
		return;
	}

	if (instruction == 'c') {
		if (tree_fd != -1) {
			close(tree_fd);
			tree_fd = -1;
			if (need_response) printf("Database file closed.\n");
		} else {
			if (need_response) printf("No database file is open.\n");
		}
		return;
	}

	if (instruction == 'v') {
		verbose_output = !verbose_output;
		if (need_response && verbose_output) printf("Verbose output enabled.\n");
		if (need_response && !verbose_output) printf("Verbose output disabled.\n");
		return;
	}

	if (tree_fd == -1 && strchr("difpltvx", instruction)) {
		if (need_response) printf("No database file is open. Please open a file first with 'o <filepath>'.\n");
		return;
	}

	switch (instruction) {
		int count;
		int64_t input_key;
		char input_value[120];
		case 'd':
			if (sscanf(command_line, "d %lld", &input_key) == 1) {
				db_delete(tree_fd, input_key);
				// print_tree disabled for performance
			}
			break;
		case 'i':
			count = sscanf(command_line, "i %lld %s", &input_key, input_value);
			if (count >= 1) {
				if (count == 1) sprintf(input_value, "%lld", input_key);
				db_insert(tree_fd, input_key, input_value);
				// print_tree disabled for performance
			}
			break;
		case 'f':
		case 'p':
			if (sscanf(command_line, "%*c %lld", &input_key) == 1) {
				find_and_print(tree_fd, input_key, instruction == 'p');
			}
			break;
		case 'l':
			print_leaves(tree_fd);
			break;
		case 't':
			print_tree(tree_fd);
			break;
		case 'x':
			db_destroy(tree_fd);
			if (need_response) printf("Tree destroyed.\n");
			break;
		default:
			if (need_help && instruction != '#') usage_2();
			break;
	}
}

// Printing tree functions
void print_tree(int fd) {
	header_page header;
	load_header_page(fd, &header);
	if(is_empty(&header)) {
		printf("Tree is empty\n");
		return;
	}

	while(print_queue.head != NULL) {
		int_pair *temp = dequeue();
		free(temp);
	}

	enqueue(make_int_pair(header.root_pn, 0));
	int current_level = 0;

	while(print_queue.head != NULL) {
		int_pair *pair = dequeue();
		pagenum_t pn = pair->first;
		int level = pair->second;

		if(level > current_level) {
			printf("\n");
			current_level = level;
		}

		page p;
		load_page(fd, pn, &p);

		for(uint32_t i = 0; i < p.num_keys; i++) {
			if(p.is_leaf) printf("%lld ", p.leaf[i].key);
			else printf("%lld ", p.internal[i].key);
		}
		printf("| ");

		if(!p.is_leaf) {
			for(uint32_t i = 0; i < p.num_keys; i++)
				enqueue(make_int_pair(p.internal[i].child, level + 1));
			enqueue(make_int_pair(p.pointer, level + 1));
		}

		free(pair);
	}
	printf("\n");
}

void print_leaves(int fd) {
	header_page header;
	load_header_page(fd, &header);
	if(is_empty(&header)) {
		printf("Tree is empty\n");
		return;
	}

	page p;
	load_page(fd, header.root_pn, &p);

	while(!p.is_leaf) {
		load_page(fd, p.internal[0].child, &p);
	}

	while(1) {
		for(int32_t i = 0; i < p.num_keys; i++)
			printf("(%llu, %s) ", p.leaf[i].key, p.leaf[i].record.value);

		if(p.pointer == -1) {
			printf("\n");
			break;
		} else {
			load_page(fd, p.pointer, &p);
		}
	}
}

void find_and_print(int fd, int64_t key, bool verbose) {
	page *leaf = NULL;
	record *r = db_find(fd, key, verbose, &leaf);
	if (r == NULL) printf("Not found.\n");
	else printf("(%lld, %s)\n", key, r->value);

	if (leaf != NULL) free(leaf);
}

// Utility functions for printing tree functions
int_pair *make_int_pair(int first, int second) {
	int_pair *new_pair = (int_pair *)malloc(sizeof(int_pair));
	new_pair->first = first;
	new_pair->second = second;
	new_pair->next = NULL;
	return new_pair;
}

void enqueue(int_pair *new_pair) {
	int_pair *cur;
	if (print_queue.head == NULL) {
		new_pair->next = NULL;
		print_queue.head = new_pair;
		print_queue.tail = new_pair;
	} else {
		new_pair->next = NULL;
		cur = print_queue.tail;
		cur->next = new_pair;
		print_queue.tail = new_pair;
	}
}

int_pair* dequeue(void) {
	int_pair *cur_head = print_queue.head;
	print_queue.head = cur_head->next;
	cur_head->next = NULL;
	return cur_head;
}

// Printing functions
void license_notice(void) {
	printf("bpt version %s -- Copyright (c) 2018  Amittai Aviram http://www.amittai.com\n", Version);
	printf("This program comes with ABSOLUTELY NO WARRANTY.\n"
			   "This is free software, and you are welcome to redistribute it\n"
			   "under certain conditions.\n"
         "Please see the headnote in the source code for details.\n");
}

void usage_1(void) {
	printf("Disk based B+ Tree.\n");
	printf("Following Silberschatz, Korth, Sidarshan, Database Concepts, "
				 "5th ed.\n\n");
}

void usage_2(void) {
	printf("Enter any of the following commands after the prompt > :\n"
	       "\to <path> [l_ord] [i_ord] -- Open a database file. Create it if not exists. 'l_ord' and 'i_ord' are optional.\n"
	       "\tc -- Close the current database file.\n"
	       "\ti <k> <v> -- Insert <k> (an integer) as key and <v> as value.\n"
	       "\ti <k> <v> -- Insert the value <v> (a string up to 119 chars) as the value of key <k> (an integer).\n"
	       "\te <filepath> [echo] [resp] -- Execute commands from a file. 'echo' and 'resp' are optional (0 for false, 1 for true, default is 0).\n"
	       "\tf <k>  -- Find the value under key <k>.\n"
	       "\tp <k> -- Print the path from the root to key k and its associated value.\n"
	       "\td <k>  -- Delete key <k> and its associated value.\n"
	       "\tx -- Destroy the whole tree.  Start again with an empty tree of the same order.\n"
	       "\tt -- Print the B+ tree.\n"
	       "\tl -- Print the keys of the leaves (bottom row of the tree).\n"
	       "\tv -- Toggle output of pointer addresses (\"verbose\") in tree and leaves.\n"
	       "\tq -- Quit. (Or use Ctl-D or Ctl-C.)\n"
	       "\t? -- Print this help message.\n");
}
