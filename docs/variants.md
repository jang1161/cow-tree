# Variant Map

이 문서는 현재 저장소의 구현 버전과 빌드 타겟을 1:1로 매핑합니다.

| ID | Source | Header | Build Target | Legacy Binary | 핵심 특징 |
|---|---|---|---|---|---|
| ram | src/variants/cow_ram.c | include/variants/cow_ram.h | make bench-ram | cow_test_ram | single writer + RAM cache |
| ram_async | src/variants/cow_ram_async.c | include/variants/cow_ram_async.h | make bench-ram_async | cow_test_ram_async | flush 병렬화 실험 |
| ram2 | src/variants/cow_ram2.c | include/variants/cow_ram2.h | make bench-ram2 | cow_test_ram2 | shard writer |
| shard | src/variants/cow_shard.c | include/variants/cow_shard.h | make bench-shard | cow_test_shard | sharded writers with per-shard routing |
| ram_stage2 | src/variants/cow_stage2.c | include/variants/cow_stage2.h | make bench-ram_stage2 | cow_test_ram_stage2 | sync/commit 2-stage |
| v3 | src/variants/cow_v3.c | include/variants/cow_v3.h | make bench-v3 | - | global cache |
| v3_multi_cache | src/variants/cow_v3_multi_cache.c | include/variants/cow_v3_multi_cache.h | make bench-v3_multi_cache | - | set-associative global cache |
| zfs | src/variants/cow_zfs.c | - (standalone) | make bench-zfs | cow_test_zfs | standalone main 포함, bench_main 비사용 |

## 공통 실행

```bash
make run-ram KEYS=1000000 MODE=0 DEV=/dev/nvme3n2
make run-zfs KEYS=1000000 MODE=64 DEV=/dev/nvme3n2
```

## 파일 정리 원칙

- 구현 파일은 `cow_<variant>.c/.h` 쌍으로 유지
- 대부분 버전은 벤치 진입점 `bench/bench_main.c`를 공유
- 예외: `zfs`는 소스 내부 `main`을 쓰는 standalone 빌드
- 빌드 결과는 반드시 `build/bin`으로 고정
- 루트에는 소스/헤더 외 생성 바이너리를 두지 않음
