CC ?= gcc
CFLAGS ?= -O2 -g -Wall -Wextra -std=c11 -pthread -Iinclude/variants -I.
LDFLAGS ?=
LIBS ?= -lzbd -lnvme -lpthread

BUILD_DIR := build
BIN_DIR := $(BUILD_DIR)/bin
BENCH_SRC := bench/bench_main.c

VARIANTS := gtx_cache gtx_cache_p

VAR_SRC_gtx_cache := src/variants/cow_gtx_cache.c
VAR_HDR_gtx_cache := include/variants/cow_gtx_cache.h
VAR_DEF_gtx_cache := COW_VARIANT_GTX_CACHE
VAR_DESC_gtx_cache := global TX + 8K-set 4-way cache

VAR_SRC_gtx_cache_p := src/variants/cow_gtx_cache_p.c
VAR_HDR_gtx_cache_p := include/variants/cow_gtx_cache.h
VAR_DEF_gtx_cache_p := COW_VARIANT_GTX_CACHE
VAR_DESC_gtx_cache_p := global TX + pwrite-backed cache

VAR_SRC_zfs := src/variants/cow_zfs.c
VAR_DESC_zfs := standalone zfs executable (has its own main)

DEFAULT_VARIANT ?= gtx_cache

RUN_GOAL := $(firstword $(MAKECMDGOALS))
ifneq (,$(filter run run-%,$(RUN_GOAL)))
RUN_POS_ARGS := $(wordlist 2,4,$(MAKECMDGOALS))
ifneq ($(strip $(RUN_POS_ARGS)),)
.PHONY: $(RUN_POS_ARGS)
$(RUN_POS_ARGS):
	@:
endif
endif

RUN_KEYS := $(or $(KEYS),$(word 1,$(RUN_POS_ARGS)),1000000)
RUN_MODE := $(or $(MODE),$(word 2,$(RUN_POS_ARGS)),0)
RUN_DEV := $(or $(DEV),$(word 3,$(RUN_POS_ARGS)),/dev/nvme3n2)

.PHONY: all
all: $(addprefix $(BIN_DIR)/cow-bench-,$(VARIANTS)) $(BIN_DIR)/cow-bench-zfs $(BIN_DIR)/cow-bench-zfs-shard-cache $(BIN_DIR)/cow-bench-zfs-gtx

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

define MAKE_VARIANT_RULE
$(BIN_DIR)/cow-bench-$(1): $(BENCH_SRC) $$(VAR_SRC_$(1)) $$(VAR_HDR_$(1)) | $(BIN_DIR)
	$$(CC) $$(CFLAGS) -D$$(VAR_DEF_$(1))=1 $(BENCH_SRC) $$(VAR_SRC_$(1)) -o $$@ $$(LDFLAGS) $$(LIBS)

.PHONY: bench-$(1)
bench-$(1): $(BIN_DIR)/cow-bench-$(1)

.PHONY: run-$(1)
run-$(1): bench-$(1)
	sudo ./$(BIN_DIR)/cow-bench-$(1) $(RUN_KEYS) $(RUN_MODE) $(RUN_DEV)
endef

$(foreach v,$(VARIANTS),$(eval $(call MAKE_VARIANT_RULE,$(v))))

$(BIN_DIR)/cow-bench-zfs: $(VAR_SRC_zfs) | $(BIN_DIR)
	$(CC) $(CFLAGS) $(VAR_SRC_zfs) -o $@ $(LDFLAGS) $(LIBS)

.PHONY: bench-zfs
bench-zfs: $(BIN_DIR)/cow-bench-zfs

.PHONY: run-zfs
run-zfs: bench-zfs
	sudo ./$(BIN_DIR)/cow-bench-zfs $(RUN_KEYS) $(RUN_MODE) $(RUN_DEV)

VAR_SRC_zfs_shard_cache := src/variants/cow_zfs_shard_cache.c
$(BIN_DIR)/cow-bench-zfs-shard-cache: $(VAR_SRC_zfs_shard_cache) | $(BIN_DIR)
	$(CC) $(CFLAGS) $(VAR_SRC_zfs_shard_cache) -o $@ $(LDFLAGS) $(LIBS)

.PHONY: bench-zfs-shard-cache
bench-zfs-shard-cache: $(BIN_DIR)/cow-bench-zfs-shard-cache

.PHONY: run-zfs-shard-cache
run-zfs-shard-cache: bench-zfs-shard-cache
	sudo ./$(BIN_DIR)/cow-bench-zfs-shard-cache $(RUN_KEYS) $(RUN_MODE) $(RUN_DEV)

VAR_SRC_zfs_gtx := src/variants/cow_zfs_gtx.c
$(BIN_DIR)/cow-bench-zfs-gtx: $(VAR_SRC_zfs_gtx) | $(BIN_DIR)
	$(CC) $(CFLAGS) $(VAR_SRC_zfs_gtx) -o $@ $(LDFLAGS) $(LIBS)

.PHONY: bench-zfs-gtx
bench-zfs-gtx: $(BIN_DIR)/cow-bench-zfs-gtx

.PHONY: run-zfs-gtx
run-zfs-gtx: bench-zfs-gtx
	sudo ./$(BIN_DIR)/cow-bench-zfs-gtx $(RUN_KEYS) $(RUN_MODE) $(RUN_DEV)

.PHONY: bench
bench: bench-$(DEFAULT_VARIANT)

.PHONY: run
run: run-$(DEFAULT_VARIANT)

.PHONY: list
list:
	@echo "Available variants:"
	@$(foreach v,$(VARIANTS),echo "  $(v) : $(VAR_DESC_$(v))";)
	@echo "  zfs : $(VAR_DESC_zfs)"
	@echo "  zfs-shard-cache : standalone zfs shard cache"
	@echo "  zfs-gtx : standalone zfs gtx"

.PHONY: compat
compat: all
	ln -sf $(BIN_DIR)/cow-bench-gtx_cache cow_test_gtx_cache
	ln -sf $(BIN_DIR)/cow-bench-gtx_cache_p cow_test_gtx_cache_p
	ln -sf $(BIN_DIR)/cow-bench-zfs cow_test_zfs
	ln -sf $(BIN_DIR)/cow-bench-zfs-shard-cache cow_test_zfs_shard_cache
	ln -sf $(BIN_DIR)/cow-bench-zfs-gtx cow_test_zfs_gtx

.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)

.PHONY: distclean
distclean: clean
	rm -f cow_test_gtx_cache cow_test_gtx_cache_p cow_test_zfs cow_test_zfs_shard_cache cow_test_zfs_gtx
