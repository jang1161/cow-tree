#pragma once

#include <stdint.h>

typedef struct record {
    char value[120];
} record;

typedef struct cow_tree cow_tree;

cow_tree *cow_open(const char *path);
void cow_close(cow_tree *t);
record *cow_find(cow_tree *t, int64_t key);
void cow_insert(cow_tree *t, int64_t key, const char *value);
