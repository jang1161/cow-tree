CC ?= gcc
CFLAGS ?= -O2 -g -Wall -Wextra -std=c11 -pthread -Iinclude/variants -I.
LDFLAGS ?=
LIBS ?= -lzbd -lnvme -lpthread

BUILD_DIR := build
BIN_DIR := $(BUILD_DIR)/bin
BENCH_SRC := bench/bench_main.c

VARIANTS := bt ram ram_async ram2 ram3 ram_stage2 v3 v3_multi_cache zfs

VAR_SRC_bt := src/variants/cow_bt.c
VAR_HDR_bt := include/variants/cow_bt.h
VAR_DEF_bt := COW_VARIANT_BT
VAR_DESC_bt := baseline bt

VAR_SRC_ram := src/variants/cow_ram.c
VAR_HDR_ram := include/variants/cow_ram.h
VAR_DEF_ram := COW_VARIANT_RAM
VAR_DESC_ram := single-writer + ram cache

VAR_SRC_ram_async := src/variants/cow_ram_async.c
VAR_HDR_ram_async := include/variants/cow_ram_async.h
VAR_DEF_ram_async := COW_VARIANT_RAM_ASYNC
VAR_DESC_ram_async := async flush pipeline

VAR_SRC_ram2 := src/variants/cow_ram2.c
VAR_HDR_ram2 := include/variants/cow_ram2.h
VAR_DEF_ram2 := COW_VARIANT_RAM2
VAR_DESC_ram2 := sharded writers

VAR_SRC_ram3 := src/variants/cow_ram3.c
VAR_HDR_ram3 := include/variants/cow_ram3.h
VAR_DEF_ram3 := COW_VARIANT_RAM3
VAR_DESC_ram3 := advanced sharded variant

VAR_SRC_ram_stage2 := src/variants/cow_ram_stage2.c
VAR_HDR_ram_stage2 := include/variants/cow_ram_stage2.h
VAR_DEF_ram_stage2 := COW_VARIANT_RAM_STAGE2
VAR_DESC_ram_stage2 := 2-stage sync/commit pipeline

VAR_SRC_v3 := src/variants/cow_v3.c
VAR_HDR_v3 := include/variants/cow_v3.h
VAR_DEF_v3 := COW_VARIANT_V3
VAR_DESC_v3 := global page cache

VAR_SRC_v3_multi_cache := src/variants/cow_v3_multi_cache.c
VAR_HDR_v3_multi_cache := include/variants/cow_v3_multi_cache.h
VAR_DEF_v3_multi_cache := COW_VARIANT_V3_MULTI_CACHE
VAR_DESC_v3_multi_cache := set-associative global cache

VAR_SRC_zfs := src/variants/cow_zfs.c
VAR_HDR_zfs := include/variants/cow_zfs.h
VAR_DEF_zfs := COW_VARIANT_ZFS
VAR_DESC_zfs := zfs-style txg pipeline

DEFAULT_VARIANT ?= ram
KEYS ?= 1000000
MODE ?= 0
DEV ?= /dev/nvme3n2

.PHONY: all
all: $(addprefix $(BIN_DIR)/cow-bench-,$(VARIANTS))

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

define MAKE_VARIANT_RULE
$(BIN_DIR)/cow-bench-$(1): $(BENCH_SRC) $$(VAR_SRC_$(1)) $$(VAR_HDR_$(1)) | $(BIN_DIR)
	$$(CC) $$(CFLAGS) -D$$(VAR_DEF_$(1))=1 $(BENCH_SRC) $$(VAR_SRC_$(1)) -o $$@ $$(LDFLAGS) $$(LIBS)

.PHONY: bench-$(1)
bench-$(1): $(BIN_DIR)/cow-bench-$(1)

.PHONY: run-$(1)
run-$(1): bench-$(1)
	sudo ./$(BIN_DIR)/cow-bench-$(1) $(KEYS) $(MODE) $(DEV)
endef

$(foreach v,$(VARIANTS),$(eval $(call MAKE_VARIANT_RULE,$(v))))

.PHONY: bench
bench: bench-$(DEFAULT_VARIANT)

.PHONY: run
run: run-$(DEFAULT_VARIANT)

.PHONY: list
list:
	@echo "Available variants:"
	@$(foreach v,$(VARIANTS),echo "  $(v) : $(VAR_DESC_$(v))";)

.PHONY: compat
compat: all
	ln -sf $(BIN_DIR)/cow-bench-bt cow_test_bt
	ln -sf $(BIN_DIR)/cow-bench-ram cow_test_ram
	ln -sf $(BIN_DIR)/cow-bench-ram_async cow_test_ram_async
	ln -sf $(BIN_DIR)/cow-bench-ram2 cow_test_ram2
	ln -sf $(BIN_DIR)/cow-bench-ram3 cow_test_ram3
	ln -sf $(BIN_DIR)/cow-bench-ram_stage2 cow_test_ram_stage2
	ln -sf $(BIN_DIR)/cow-bench-zfs cow_test_zfs

.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)

.PHONY: distclean
distclean: clean
	rm -f cow_test_bt cow_test_ram cow_test_ram_async cow_test_ram2 cow_test_ram3 cow_test_ram_stage2 cow_test_zfs
