#include "file_manager.h"
#include "dbbpt.h"

#include <stdbool.h>
#ifdef _WIN32
#define bool char
#define false 0
#define true 1
#endif
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>

// FUNCTION DEFINITIONS.
// Find API
/**
 * @brief Find a record with the given key.
 * @param fd[in] The file descriptor of the database file.
 * @param key[in] The key to find.
 * @param verbose[in] Whether to print verbose output.
 * @param leaf_out[out] The leaf page pointer where the key is found.
 * @return The record with the given key, or NULL if not found.
 */
record *db_find(int fd, int64_t key, bool verbose, page **leaf_out) {
	header_page header;
	load_header_page(fd, &header);
	if(is_empty(&header)) {
		printf("Tree is empty\n");
		return NULL;
	}
	
	page p;
	load_page(fd, header.root_pn, &p);
	
	pagenum_t leaf_pn = find_leaf(fd, &p, key, verbose);
	load_page(fd, leaf_pn, &p); // now p is leaf page
	
	for(uint32_t i = 0; i < p.num_keys; i++) {
		if(key == p.leaf[i].key) {
			*leaf_out = (page *)malloc(sizeof(page));
			if(!*leaf_out) {
				printf("db_find malloc error\n");
				return NULL;
			}
			**leaf_out = p;
			return &((*leaf_out)->leaf[i].record);
		}
	}

	return NULL;
}

// Helper functions for find API

// Insertion API
/**
 * @brief Insert a record with the given key and value.
 * @param fd[in] The file descriptor of the database file.
 * @param key[in] The key to insert.
 * @param value[in] The value to insert.
 */
void db_insert(int fd, int64_t key, char *value) {
	header_page header;
	load_header_page(fd, &header);

	// tree is empty, make new root page
	if(is_empty(&header)) {
		page *root = alloc_page(fd);
		load_header_page(fd, &header);

		root->parent_pn = 0;
		root->is_leaf = 1;
		root->num_keys = 1;
		root->pointer = -1;
		root->leaf[0].key = key;
		memcpy(root->leaf[0].record.value, value, 120);

		header.root_pn = root->my_pn;

		write_page(fd, root);
		write_header_page(fd, &header);
		return;
	}

	page p;
	load_page(fd, header.root_pn, &p);
	pagenum_t leaf_pn = find_leaf(fd, &p, key, 0);
	load_page(fd, leaf_pn, &p);

	// leaf page is not full
	if(!is_full(&header, &p)) {
		uint32_t pos = get_position(&p, key);

		for (int64_t i = p.num_keys - 1; i >= (int64_t)pos; i--)
			p.leaf[i+1] = p.leaf[i];

		p.leaf[pos].key = key;
		memcpy(p.leaf[pos].record.value, value, 120);
		p.num_keys++;
		write_page(fd, &p);
		return;
	}

	// leaf is full, split
	pagenum_t new_root_pn = insert_and_split_leaf(fd, &p, key, value);

	if(new_root_pn != -1) { // root has benn changed
		load_header_page(fd, &header);
		header.root_pn = new_root_pn;
		write_header_page(fd, &header);
	}
}

// Helper functions for insertion API

// called when leaf is aleady full
// return new root pn, or -1 if root is not changed
pagenum_t insert_and_split_leaf(int fd, page *leaf, int64_t key, char *value) {
	header_page header;
	load_header_page(fd, &header);

	uint32_t order = header.leaf_order; // current key num: order-1
	leaf_entity *tmp = (leaf_entity *)malloc(order * sizeof(leaf_entity)); // current key num + 1

	uint32_t pos = 0;	// position for new key
	while(pos < leaf->num_keys && leaf->leaf[pos].key < key)
		pos++;

	uint32_t i;
	for(i = 0; i < pos; i++)
		tmp[i] = leaf->leaf[i];
	for(i = pos; i < leaf->num_keys; i++)
		tmp[i+1] = leaf->leaf[i];

	tmp[pos].key = key;
	memcpy(tmp[pos].record.value, value, 120);

	uint32_t split = order / 2;

	page *new_leaf = alloc_page(fd);
	new_leaf->is_leaf = 1;
	new_leaf->parent_pn = leaf->parent_pn;

	for(i = 0; i < split; i++)
		leaf->leaf[i] = tmp[i];
	leaf->num_keys = split;

	for(i = 0; i < order-split; i++)
		new_leaf->leaf[i] = tmp[split+i];
	new_leaf->num_keys = order - split;

	new_leaf->pointer = leaf->pointer;
	leaf->pointer = new_leaf->my_pn;

	free(tmp);
	write_page(fd, leaf);
	write_page(fd, new_leaf);

	int64_t promote_key = new_leaf->leaf[0].key;
	return insert_into_parent(fd, leaf, promote_key, new_leaf);
}

pagenum_t insert_and_split_internal(int fd, page *p, uint32_t left_idx, int64_t key, pagenum_t right_pn) {
	header_page header;
	load_header_page(fd, &header);
	
	uint32_t order = header.internal_order;
	int64_t *tmp_keys = (int64_t *)malloc(order * sizeof(int64_t));
	pagenum_t *tmp_children = (pagenum_t *)malloc((order + 1) * sizeof(pagenum_t));

	uint32_t i;
	for(i = 0; i < left_idx; i++)
		tmp_keys[i] = p->internal[i].key;
	for(i = left_idx; i < p->num_keys; i++)
		tmp_keys[i+1] = p->internal[i].key;
	tmp_keys[left_idx] = key;

	if(left_idx == p->num_keys) { // left was pointer
		for(i = 0; i < p->num_keys; i++)
			tmp_children[i] = p->internal[i].child;
		tmp_children[p->num_keys] = p->pointer;
		tmp_children[p->num_keys+1] = right_pn;
	} else {
		for(i = 0; i <= left_idx; i++)
			tmp_children[i] = p->internal[i].child;
		for(i = left_idx+1; i < p->num_keys; i++)
			tmp_children[i+1] = p->internal[i].child;
		tmp_children[left_idx+1] = right_pn;
		tmp_children[p->num_keys+1] = p->pointer;
	}

	uint32_t split = (order + 1) / 2;
	int64_t promote_key = tmp_keys[split-1];

	page *new_p = alloc_page(fd);
	new_p->is_leaf = 0;
	new_p->parent_pn = p->parent_pn;

	// promote key is not involved in child node when internal node is splited
	for(i = 0; i < split-1; i++) {
		p->internal[i].key = tmp_keys[i];
		p->internal[i].child = tmp_children[i];
	}
	p->pointer = tmp_children[split-1];
	p->num_keys = split - 1;

	for(i = split; i < order; i++) {
		new_p->internal[i-split].key = tmp_keys[i];
		new_p->internal[i-split].child = tmp_children[i];
	}
	new_p->pointer = tmp_children[order];
	new_p->num_keys = order - split;

	page child;
	for(i = 0; i < new_p->num_keys; i++) {
		load_page(fd, new_p->internal[i].child, &child);
		child.parent_pn = new_p->my_pn;
		write_page(fd, &child);
	}
	load_page(fd, new_p->pointer, &child);
	child.parent_pn = new_p->my_pn;
	write_page(fd, &child);

	free(tmp_keys);
	free(tmp_children);

	write_page(fd, p);
	write_page(fd, new_p);

	return insert_into_parent(fd, p, promote_key, new_p);
}

// return new root pn, or -1 if root is not changed
pagenum_t insert_into_parent(int fd, page *left, int64_t key, page *right) {
	header_page header;
	load_header_page(fd, &header);

	if(is_root(left)) { // left was the root -> create new root
		page *new_root = alloc_page(fd);

		new_root->parent_pn = 0;
		new_root->is_leaf = 0;
		new_root->num_keys = 1;
		new_root->internal[0].key = key;
		new_root->internal[0].child = left->my_pn;
		new_root->pointer = right->my_pn;

		left->parent_pn = new_root->my_pn;
		right->parent_pn = new_root->my_pn;

		write_page(fd, left);
		write_page(fd, right);
		write_page(fd, new_root);

		return new_root->my_pn;
	}

	// parent exist
	page parent;
	load_page(fd, left->parent_pn, &parent);
	right->parent_pn = parent.my_pn;
	write_page(fd, right);

	int64_t pos = get_position(&parent, key);
	
	// parent is not full
	if(!is_full(&header, &parent)) {
		for(int64_t i = parent.num_keys-1; i >= pos; i--)
			parent.internal[i+1] = parent.internal[i];

		parent.internal[pos].key = key;
		parent.internal[pos].child = left->my_pn;

		if(pos == parent.num_keys)
			parent.pointer = right->my_pn;
		else
			parent.internal[pos+1].child = right->my_pn;
		
		parent.num_keys++;
		write_page(fd, &parent);
		return -1; // no root change
	}

	// parent is aleady full -> recursive split
	uint32_t left_idx = get_position(&parent, key);
	return insert_and_split_internal(fd, &parent, left_idx, key, right->my_pn);
}

// Deletion API
/**
 * @brief Delete a record with the given key.
 * @param fd[in] The file descriptor of the database file.
 * @param key[in] The key to delete.
 */
void db_delete(int fd, int64_t key) {
	header_page header;
	load_header_page(fd, &header);
	if(is_empty(&header)) {
		printf("Tree is empty\n");
		return;
	}

	page p;
	load_page(fd, header.root_pn, &p);

	bool is_leftmost = true;
	while(!p.is_leaf) {
		uint32_t idx = -1;
		for(uint32_t i = 0; i < p.num_keys; i++) {
			if(key < p.internal[i].key) {
				idx = i;
				break;
			}
		}
		if(idx == -1) 
			load_page(fd, p.pointer, &p); // right most child
		else 
			load_page(fd, p.internal[idx].child, &p);
		if(idx != 0) is_leftmost = false;
	}

	uint32_t pos = -1;
	for(uint32_t i = 0; i < p.num_keys; i++) {
		if(p.leaf[i].key == key) {
			pos = i;
			break;
		}
	}

	if(pos == -1) {
		printf("Key %lld does not exist in the tree\n", key);
		return;
	}

	bool exist_in_index = !is_root(&p) && pos == 0 && !is_leftmost;

	uint32_t leaf_min = (header.leaf_order - 1) / 2; // minimun number of keys in leaf

	// delete the key
	for(uint32_t i = pos; i < p.num_keys - 1; i++)
		p.leaf[i] = p.leaf[i+1];
	p.num_keys--;
	write_page(fd, &p);

	if(p.num_keys >= leaf_min || is_root(&p)) {
		// if root leaf becomes empty, make tree empty
		if(is_root(&p) && p.num_keys == 0) {
			header.root_pn = -1;
			write_header_page(fd, &header);
			free_page(fd, p.my_pn);
			return;
		}
		if(exist_in_index)
			update_internal_key(fd, &p, key, p.leaf[0].key);
		return;
	}

	// underflow
	page left, right;
	pagenum_t left_pn = find_left_sibling(fd, &p);
	pagenum_t right_pn = find_right_sibling(fd, &p);
	
	if(left_pn != -1) 
		load_page(fd, left_pn, &left);
	if(right_pn != -1) 
		load_page(fd, right_pn, &right);

	// borrow one from one of siblings
	if(left_pn != -1 && left.num_keys > leaf_min) {
		borrow_from_left(fd, &p, &left);
		load_page(fd, p.my_pn, &p);
		if(exist_in_index)
			update_internal_key(fd, &p, key, p.leaf[0].key);
	} 
	else if(right_pn != -1 && right.num_keys > leaf_min) {
		borrow_from_right(fd, &p, &right);
		load_page(fd, p.my_pn, &p);
		if(exist_in_index)
			update_internal_key(fd, &p, key, p.leaf[0].key);
	}
	
	// merge
	else{
		page parent;
		load_page(fd, p.parent_pn, &parent);
		uint32_t internal_min = (header.internal_order - 1) / 2;

		if(left_pn != -1) {
			leaf_merge(fd, &left, &p);
			delete_internal_key(fd, &p);
			free_page(fd, p.my_pn);
			load_page(fd, left_pn, &p);
		} else {
			leaf_merge(fd, &p, &right);
			delete_internal_key(fd, &right);
			free_page(fd, right_pn);
			load_page(fd, p.my_pn, &p);
			
			if(exist_in_index)
          		update_internal_key(fd, &p, key, p.leaf[0].key);
		}

		load_page(fd, parent.my_pn, &parent);
		while(parent.num_keys < internal_min) {
			if(is_root(&parent)) {
				if(parent.num_keys == 0) {
					page new_root;
					load_page(fd, parent.pointer, &new_root);
					new_root.parent_pn = 0;
					header.root_pn = parent.pointer;

					write_page(fd, &new_root);
					write_header_page(fd, &header);
					free_page(fd, parent.my_pn);
				}
				break;
			}

			internal_redistribute(fd, &header, &parent);
			load_page(fd, parent.parent_pn, &parent);
		}

	}
}

// Helper functions for delete API

void update_internal_key(int fd, page *p, int64_t deleted_key, int64_t new_key) {
	page pp;
	load_page(fd, p->parent_pn, &pp);

	while(1) {
		for(uint32_t i = 0; i < pp.num_keys; i++) {
			if(pp.internal[i].key == deleted_key) {
				pp.internal[i].key = new_key;
				write_page(fd, &pp);
				return;
			}
		}

		if(is_root(&pp)) return;
		load_page(fd, pp.parent_pn, &pp);
	}
}

pagenum_t find_left_sibling(int fd, page *p) {
	page pp;
	load_page(fd, p->parent_pn, &pp);

	if(pp.pointer == p->my_pn)
		return pp.internal[pp.num_keys-1].child;

	for(uint32_t i = 0; i < pp.num_keys; i++) {
        if(pp.internal[i].child == p->my_pn) {
            if(i == 0) return -1;
            return pp.internal[i-1].child;
        }
    }
	printf("find_left_sibling() error\n");
    return -1;
}

pagenum_t find_right_sibling(int fd, page *p) {
	page pp;
	load_page(fd, p->parent_pn, &pp);

	if(pp.pointer == p->my_pn)
		return -1;

	for(uint32_t i = 0; i < pp.num_keys; i++) {
        if(pp.internal[i].child == p->my_pn) {
            if(i == pp.num_keys - 1) return pp.pointer;
            return pp.internal[i+1].child;
        }
    }
	printf("find_right_sibling() error\n");
    return -1;
}

void borrow_from_left(int fd, page *p, page *left) {
	for(int64_t i = (int64_t)p->num_keys - 1; i >= 0; i--)
		p->leaf[i+1] = p->leaf[i];
	p->leaf[0] = left->leaf[left->num_keys-1];

	p->num_keys++;
	left->num_keys--;

	page pp;
	load_page(fd, p->parent_pn, &pp);
	for(uint32_t i = 0; i < pp.num_keys; i++) {
		if(pp.internal[i].child == left->my_pn)
			pp.internal[i].key = p->leaf[0].key;
	}

	write_page(fd, p);
	write_page(fd, left);
	write_page(fd, &pp);
}

void borrow_from_right(int fd, page *p, page *right) {
	p->leaf[p->num_keys] = right->leaf[0];
	for(uint32_t i = 0; i < right->num_keys - 1; i++)
		right->leaf[i] = right->leaf[i+1];

	p->num_keys++;
	right->num_keys--;

	page pp;
	load_page(fd, p->parent_pn, &pp);
	for(uint32_t i = 0; i < pp.num_keys; i++) {
		if(pp.internal[i].child == p->my_pn)
			pp.internal[i].key = right->leaf[0].key;
	}

	write_page(fd, p);
	write_page(fd, right);
	write_page(fd, &pp);
}

void leaf_merge(int fd, page *left, page *right) {
	for(uint32_t i = 0; i < right->num_keys; i++)
		left->leaf[left->num_keys+i] = right->leaf[i];

	left->num_keys += right->num_keys;
	left->pointer = right->pointer;
	write_page(fd, left);
}

void delete_internal_key(int fd, page *p) {
	page pp;
	load_page(fd, p->parent_pn, &pp);

	if(pp.pointer == p->my_pn) {
		pp.pointer = pp.internal[pp.num_keys-1].child;
		pp.num_keys--;
		write_page(fd, &pp);
		return;
	}

	uint32_t pos;
	for(pos = 0; pos < pp.num_keys; pos++) {
		if(pp.internal[pos].child == p->my_pn)
			break;
	}

	if(pos == 0)
		for(uint32_t i = 0; i < pp.num_keys - 1; i++)
			pp.internal[i].key = pp.internal[i+1].key;
	else
		for(uint32_t i = pos - 1; i < pp.num_keys - 1; i++)
			pp.internal[i].key = pp.internal[i+1].key;

	for(uint32_t i = pos; i < pp.num_keys - 1; i++)
		pp.internal[i].child = pp.internal[i+1].child;
	pp.num_keys--;

	write_page(fd, &pp);
}

void internal_redistribute(int fd, header_page *header, page *p) {
	uint32_t internal_t = (header->internal_order + 1) / 2;

	page left, right;
	pagenum_t left_pn = find_left_sibling(fd, p);
	pagenum_t right_pn = find_right_sibling(fd, p);
	if(left_pn != -1) load_page(fd, left_pn, &left);
	if(right_pn != -1) load_page(fd, right_pn, &right);

	page pp;
	load_page(fd, p->parent_pn, &pp);

	uint32_t parent_key_idx = -1;
	int64_t parent_key;
	for(uint32_t i = 0; i < pp.num_keys; i++) {
		if(pp.internal[i].child == p->my_pn) {
			parent_key_idx = i;
			parent_key = pp.internal[i].key;
			break;
		}
	}
	if(parent_key_idx == -1) // p was the rightmost child
		parent_key_idx = pp.num_keys;
	
	// borrow from left
	if(left_pn != -1 && left.num_keys >= internal_t) { 
		for(int64_t i = (int64_t)p->num_keys - 1; i >= 0; i--) // shift existing keys to the right
			p->internal[i+1] = p->internal[i];

		p->internal[0].key = pp.internal[parent_key_idx-1].key;
		p->internal[0].child = left.pointer;

		pp.internal[parent_key_idx-1].key = left.internal[left.num_keys-1].key;
		left.pointer = left.internal[left.num_keys-1].child;

		p->num_keys++;
		left.num_keys--;

		page child;
		load_page(fd, p->internal[0].child, &child);
		child.parent_pn = p->my_pn;
		
		write_page(fd, p);
		write_page(fd, &left);
		write_page(fd, &pp);
		write_page(fd, &child);

		return;
	} 
	
	// borrow from right
	else if(right_pn != -1 && right.num_keys >= internal_t) { 
		p->internal[p->num_keys].key = parent_key;
		p->internal[p->num_keys].child = p->pointer;
		p->pointer = right.internal[0].child;

		pp.internal[parent_key_idx].key = right.internal[0].key;

		for(uint32_t i = 0; i < right.num_keys - 1; i++)
			right.internal[i] = right.internal[i+1];
		
		p->num_keys++;
		right.num_keys--;

		page child;
		load_page(fd, p->pointer, &child);
		child.parent_pn = p->my_pn;

		write_page(fd, p);
		write_page(fd, &right);
		write_page(fd, &pp);
		write_page(fd, &child);

		return;
	}
	
	// merge with left
	if(left_pn != -1) { 
		internal_merge(fd, &pp, parent_key_idx-1, &left, p);
		free_page(fd, p->my_pn);
		load_page(fd, left_pn, p);
	} 
	// merge with right
	else { 
		internal_merge(fd, &pp, parent_key_idx, p, &right);
		free_page(fd, right_pn);
		load_page(fd, p->my_pn, p);
	}

}

void internal_merge(int fd, page *parent, uint32_t pki, page *left, page *right) { // pki: parent key index
	// take a key from parent
	left->internal[left->num_keys].key = parent->internal[pki].key;
	left->internal[left->num_keys].child = left->pointer;
	left->num_keys++;
	
	// parent key update
	for(uint32_t i = pki; i < parent->num_keys - 1; i++)
		parent->internal[i] = parent->internal[i+1];
	parent->num_keys--;
	parent->internal[pki].child = left->my_pn;

	if(parent->pointer == right->my_pn)
		parent->pointer = left->my_pn;

	// merge right to left
	page child;
	for(uint32_t i = 0; i < right->num_keys; i++) {
		left->internal[left->num_keys+i] = right->internal[i];
		load_page(fd, right->internal[i].child, &child);
		child.parent_pn = left->my_pn;
		write_page(fd, &child);
	}
	
	left->pointer = right->pointer;
	load_page(fd, right->pointer, &child);
	child.parent_pn = left->my_pn;
	write_page(fd, &child);

	left->num_keys += right->num_keys;

	write_page(fd, left);
	write_page(fd, parent);
}

// Destroy API
void db_destroy(int fd) {

}

// Helper functions for destroy API
void destroy_pages(int fd, int64_t pgn) {
	free_page(fd, pgn);
}

// Utility helper functions
pagenum_t find_leaf(int fd, page *p, int64_t key, int verbose) {
	while(!p->is_leaf) {
		if(verbose) printf("internal page: %llu -> ", p->my_pn);

		uint32_t idx = -1;
		for(uint32_t i = 0; i < p->num_keys; i++) {
			if(key < p->internal[i].key) {
				idx = i;
				break;
			}
		}
		if(idx == -1) load_page(fd, p->pointer, p); // right most child
		else load_page(fd, p->internal[idx].child, p);
	}
	if(verbose) printf("leaf page: %llu\n", p->my_pn);
	return p->my_pn;
}

uint32_t get_position(page *p, int64_t key) {
	if(p->is_leaf) { // for leaf
		for(uint32_t i = 0; i < p->num_keys; i++)
			if(key < p->leaf[i].key)
				return i;
	} else { // for internal
		for(uint32_t i = 0; i < p->num_keys; i++)
			if(key < p->internal[i].key)
				return i;
	}
	return p->num_keys;
}

bool is_empty(header_page *header) {
	return header->root_pn == -1;
}

bool is_root(page *p) {
	return p->parent_pn == 0;
}

bool is_full(header_page *h, page *p) {
	uint32_t capacity = p->is_leaf ? h->leaf_order - 1 : h->internal_order - 1;
	return p->num_keys == capacity;
}

// bool underflow(header_page *h, page *p, bool leaf) {
// 	if(is_root(p)) return 0;

// 	u_int32_t min = leaf ? (h->leaf_order - 1) / 2 : (h->internal_order - 1) / 2;
// 	return p->num_keys < min;
// }