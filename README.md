# COW Tree ZNS Benchmark

여러 COW B-tree 구현(버전)을 동일 워크로드로 비교하기 위한 실험 저장소입니다.

## 현재 저장소 구조

- `src/variants/`: 버전별 C 구현(`cow_*.c`)
- `include/variants/`: 버전별 헤더(`cow_*.h`)
- `bench/bench_main.c`: 공통 벤치 진입점(버전별로 재사용)
- `benchmark/`: 배치 실행/결과 수집 스크립트
- `results/`: 측정 결과 로그
- `refs/`: 외부 참고 구현
- `docs/`: 버전 매핑, 정리 문서

## 빌드 (통합)

이 저장소는 버전별 바이너리를 `build/bin` 아래로 통합 생성합니다.

1. 전체 빌드

```bash
make all
```

2. 특정 버전만 빌드

```bash
make bench-ram
make bench-zfs
make bench-ram_stage2
```

3. 사용 가능한 버전 목록

```bash
make list
```

4. 기존 이름(`cow_test_*`) 호환 심볼릭 링크 생성

```bash
make compat
```

## 실행

1. 특정 버전 실행

```bash
make run-ram KEYS=1000000 MODE=0 DEV=/dev/nvme3n2
```

2. 기본 실행(기본 버전: `ram`)

```bash
make run KEYS=1000000 MODE=0 DEV=/dev/nvme3n2
```
또는
```bash
make run 1000000 0 /dev/nvme3n2
```

인자 의미:

- `KEYS`: 키 개수
- `MODE`: `0`이면 전체 스레드(1/2/4/8/16/32/64), 아니면 단일 스레드 수
- `DEV`: ZNS 디바이스 경로

## 버전 매핑

버전별 구현 파일, 빌드 타겟, 특징은 아래 문서로 정리했습니다.

- `docs/variants.md`
- `version_quick_guide.md`
- `summary.md`

## 벤치 스크립트

- `benchmark/run_bench.sh`
	- 실행 방식 1(라벨형): `./benchmark/run_bench.sh <variant> 100K=<n> 1M=<n> 10M=<n> DEV=<path> [OUTFILE=<path>]`
	- 실행 방식 2(숫자형): `./benchmark/run_bench.sh <variant> <repeat_100K> <repeat_1M> <repeat_10M> [device] [outfile]`
	- 예시 1: `./benchmark/run_bench.sh ram 100K=3 1M=3 10M=1 DEV=/dev/nvme3n2`
	- 예시 2: `./benchmark/run_bench.sh ram 3 3 1 /dev/nvme3n2`
	- 동작: 각 키 크기(100K/1M/10M)에 대해 `make run-<variant> <keys> 0 <dev>`를 지정 횟수만큼 반복 실행 후 스레드별 평균 throughput 계산
	- 결과 파일은 처리량 요약만 저장하며, `OUTFILE` 미지정 시 기본값은 `results/<variant>.txt` (예: `results/ram.txt`)