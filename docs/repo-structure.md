# Repository Structure Guide

현재는 소스/헤더를 실제로 분리해 `src/variants`, `include/variants` 구조를 적용했습니다.

## 적용된 구조

- `bench/`: 공통 벤치 엔트리
- `docs/`: 버전/구조 문서
- `build/bin/`: 통합 빌드 출력
- `benchmark/`: 반복 실행 스크립트
- `results/`: 결과 로그
- `src/variants/`: 버전별 C 소스
- `include/variants/`: 버전별 헤더

## 유지 가이드

현재 구조를 유지하면서 확장할 때 아래 원칙을 따르는 것을 권장합니다.

```text
src/variants/
  cow_ram.c
  cow_ram_async.c
  ...
include/variants/
  cow_ram.h
  cow_ram_async.h
  ...
bench/
  bench_main.c
scripts/
  run_bench.sh
results/
  *.txt
docs/
  *.md
```

## 네이밍 규칙 추천

- 구현 ID: 소문자 + snake_case (`ram_stage2`, `v3_multi_cache`)
- 소스 파일: `cow_<id>.c`, `cow_<id>.h`
- 실행 파일: `cow-bench-<id>`
- 결과 파일: `<id>_<dataset>.txt` (예: `ram_stage2_1M.txt`)
