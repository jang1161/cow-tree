# COW Tree 버전 빠른 가이드

이 문서는 현재 레포의 주요 구현들이 무엇을 실험한 버전인지 빠르게 파악하기 위한 요약입니다.

## 1) no-use/cow_tree_v1

- 한 줄 요약: 초기 COW 기준 구현. 스냅샷 루트/시퀀스를 읽고, 새 루트를 만든 뒤 publish 시점에 commit lock으로 원자적 교체.
- 읽기 경로: find 시점에 root+seq 스냅샷을 읽고 해당 루트 기준으로 탐색.
- 쓰기 경로: 호출 스레드가 직접 leaf split/상위 propagate를 수행해서 candidate root 생성.
- publish/락: 마지막에 commit lock을 잡고 "기대(root, seq)"와 같으면 superblock을 디스크에 쓰며 커밋.
- 성격: 단순하고 이해하기 쉬운 베이스라인(동시성은 publish 구간 mutex 중심).

## 2) cow_ram

- 한 줄 요약: 단일 writer 배치 처리 + RAM 페이지 캐시를 붙인 기본 RAM 최적화 버전.
- 읽기 경로: thread-local read cache + RAM hash table cache + O_DIRECT read fallback.
- 쓰기 경로: insert 요청을 큐에 넣고 writer thread가 배치 정렬 후 overlay 트리를 만들어 한 번에 flush.
- publish/락: volatile superblock seq를 odd/even으로 토글해서 lock-free 스냅샷 읽기, 주기적 flusher가 durable superblock 반영.
- 성격: write를 비동기 큐로 직렬화해서 경합을 낮추는 1-writer 구조.

## 3) cow_ram_internal

- 한 줄 요약: cow_ram에서 RAM 캐시 대상을 내부 노드 중심으로 제한한 버전.
- 핵심 차이: ram_cache_eligible 조건으로 내부 노드(is_leaf == 0)만 RAM table에 올림.
- 의도: 리프 churn(잦은 갱신)로 인한 캐시 오염을 줄이고 탐색 경로(내부 노드) hit를 안정화.
- 그 외 구조: writer 배치/overlay flush/publish/flusher는 cow_ram과 동일 계열.

## 4) cow_ram_async

- 한 줄 요약: cow_ram 계열에서 overlay flush를 레벨 단위 병렬 작업으로 확장한 버전.
- 읽기 경로: thread-local + RAM table 캐시(ram_table_lock 사용).
- 쓰기 경로: writer가 배치 적용 후 flush_overlay_async를 통해 하위 레벨부터 병렬 flush.
- publish/락: publish는 single-writer seq 토글 방식 유지, durable 반영은 별도 flusher thread.
- 성격: 트리 flush 구간의 병렬성을 높여 배치 flush 지연을 줄이려는 실험 버전.

## 5) cow_ram2

- 한 줄 요약: key sharding 기반 멀티 writer 버전(WRITER_SHARDS=8).
- 읽기 경로: key -> shard 매핑 후 해당 shard root 스냅샷으로 조회.
- 쓰기 경로: shard별 큐/writer thread가 독립 배치 처리 후 shard root를 publish.
- publish/락: shard_root_pn/shard_seq_no를 개별 갱신하고, 전역 volatile superblock은 shard 0 경로 중심으로 반영.
- 캐시/락 차이: RAM table 락을 mutex 대신 pthread_rwlock으로 분리(read/write lock).
- 성격: writer 병렬성 확대가 목적이며, 전역 루트 표현은 단일 트리 버전들과 다르게 shard 관점.

## 6) cow_v3

- 한 줄 요약: 단일 writer 구조 + 전역 페이지 캐시(global page cache) 추가 버전.
- 읽기 경로: thread-local cache 이후 global cache(슬롯별 mutex 보호) 조회, miss 시 O_DIRECT read.
- 쓰기 경로: writer 배치 + overlay flush + single-root publish(기본 골격은 ram 계열과 유사).
- 성격: RAM table 대신(또는 보완) 전역 공유 캐시로 스레드 간 read reuse를 노린 버전.

## 7) cow_v3_multi-cache

- 한 줄 요약: cow_v3의 전역 캐시를 set-associative multi-way로 확장한 버전.
- 캐시 구조: GLOBAL_PAGE_CACHE_SETS x GLOBAL_PAGE_CACHE_WAYS(4-way), set lock + RR victim 교체.
- 읽기 경로: set 내 way 탐색으로 hit 판단, miss/eviction 통계 수집.
- 쓰기 경로: writer 배치/publish는 v3 계열 동일, 차이는 캐시 충돌 완화 전략.
- 성격: 단일 슬롯 해시 충돌 문제를 줄여 고경합 read workload에서 hit율/지연 개선을 노린 버전.
