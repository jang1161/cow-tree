# Cow Tree Experiment Timeline and Results

This document summarizes the implementation timeline and benchmark outcomes in chronological order.

## 1) cow_v3 baseline
- Result file: results/result_v3.txt
- Model: single writer with request queue and batching
- Behavior:
  - Throughput scales with threads, but large key counts show limited scaling due to single-writer serialization and storage-read pressure.

Selected average throughput from results/result_v3.txt
- 10K: 1T 7030, 64T 20640
- 100K: 1T 4291, 64T 10094
- 1M: 1T 3645, 64T 6231

## 2) cow_v3_multi_cache (also referred to as vow_v3_multi_cache in discussion)
- Result file: results/multi-cache_10M.txt
- Model: 8K set x 4-way global cache with instrumentation
- Behavior:
  - Small datasets benefit strongly from cache.
  - As key count grows, cache hit ratio drops and O_DIRECT reads remain high.
  - At 10M, scaling exists but gain is moderate.

Selected throughput and cache trend from results/multi-cache_10M.txt
- 10K: 1T 7761.74, 64T 23254.82, hit ratio 100.00%
- 1M: 1T 5128.76, 64T 8862.18, hit ratio 81.82% -> 73.07%
- 10M: 1T 3747.73, 64T 5879.97, hit ratio 75.09% -> 57.12%

## 3) cow_ram (RAM-first)
- Result file: results/ram.txt
- Model: RAM-resident page table first, append updates to ZNS
- Behavior:
  - Large-key performance improves significantly compared with multi-cache.
  - Better scaling at 1M and 10M due to reduced read misses to storage.
  - Profile 핵심:
    - append latency(app_us)는 거의 고정이며 storage 자체 병목은 크지 않음
    - writer batch latency(bt_us)는 스레드 증가 시 급증해 CPU 경로 병목이 커짐
    - insert q_lock 대기(qins_us) 증가로 producer 경합이 커짐
    - append_retries/zone_rotations는 거의 0으로 zone 전환 병목은 상대적으로 작음

Selected average throughput from results/ram.txt
- 10K: 1T 8422, 64T 27031
- 100K: 1T 7953, 64T 20820
- 1M: 1T 8946, 64T 15429
- 10M: 1T 7434, 64T 13287 (single run logged)

## 4) cow_ram_shard (aborted mid-run)
- Result file: results/ram_shard.txt
- Model: multiple shard queues/writers, but commit still serialized with commit_lock
- Behavior:
  - 10K completed, but significantly underperformed cow_ram.
  - Run was stopped during 100K Run#1.

Observed averages from completed 10K section
- 10K: 1T 4466, 64T 17984

Partial 100K section
- Only Run#1 entries up to 16T are present in the file.

## 5) cow_ram_latch (aborted mid-run)
- Result file: results/ram_latch.txt
- Model: shard writers plus latch stripes, still with commit_lock serialization
- Behavior:
  - 10K completed, but underperformed cow_ram.
  - Additional latch overhead did not offset serialization cost.
  - Run was stopped during 100K Run#1.

Observed averages from completed 10K section
- 10K: 1T 5065, 64T 17019

Partial 100K section
- Only Run#1 entries for 1T and 2T are present.

## 6) cow_ram2 (Shard-based parallel writer)
- Result file: results/ram2.txt
- Code files: cow_ram2.c, cow_ram2.h
- Model:
  - 8 shard queues/writers with key-based routing
  - per-shard root publish (global commit_lock 제거)
  - shared RAM table with RWlock protection
  - single active data zone rollover policy
- Current status:
  - 10K/100K/1M: complete
  - 10M: Run#1 only, and 1-thread value is invalid (0) due to run/parsing failure in that run

## 7) cow_ram2 result summary
- Applied:
  - 8 shard writer queues
  - per-shard root publish (commit_lock 제거)
  - shared RAM table with RWlock
- Result:
  - 10K/100K/1M에서는 고스레드 구간에서 ram 대비 이득이 있었음
  - 저스레드(1T~2T)는 ram 대비 손해
  - 10M은 중간 스레드 일부만 우세, 고스레드에서는 다시 ram이 우세
  - 10M 1T 값은 invalid(0)라 재측정 필요

## 8) cow_ram_nobatch result summary
- Applied:
  - 배칭 제거
  - insert마다 즉시 leaf~root append
  - 단일 insert lock 직렬화
- Result (100K test):
  - 1T는 높은 처리량이 나왔지만
  - 스레드 증가 시 insert lock 대기로 급격히 하락
  - 64T에서 크게 저하되어 확장성은 낮음

## 9) cow_ram_latch result summary
- Applied:
  - insert마다 즉시 append는 유지
  - latch crabbing 기반 동시 수정 시도
- Result (100K test):
  - 데드락 이슈는 보완 후 완료 가능
  - 하지만 전체적으로 ram 대비 확장성 개선 효과는 작음
  - 고스레드에서 처리량 하락이 큼

## 10) cow_ram3 (new)
- Code files: cow_ram3.c, cow_ram3.h
- Applied:
  - ram1 기반 single writer 유지
  - queue를 sharded queue(QUEUE_SHARDS=8)로 분산
  - adaptive batching: queue depth 기반 wait time 조절
- Result (current):
  - 저스레드(1T~4T)에서 ram1 대비 크게 느림
  - 8T~64T 구간에서도 대부분 ram1보다 낮거나 비슷한 수준
  - 현재 구성에서는 queue/batch 개선보다 관리 오버헤드가 더 큰 것으로 보임

## 11) cow_ram_intermal (new)
- Code files: cow_ram_intermal.c, cow_ram_intermal.h
- Applied:
  - RAM cache 정책 변경: root/internal만 RAM table 캐시
  - leaf 페이지는 기본적으로 RAM table에 유지하지 않고 필요 시 로드
  - 오버레이 flush/계측/통계 구조는 ram1 기반 유지
- Goal:
  - RAM footprint를 줄이고 내부 노드 탐색 hot-path 효율 중심으로 비교 실험

## 12) ref3_bt reference architecture (refs/ref3_bt)
- 핵심 구조만 요약:
  - insert/lookup/remove는 tree read lock으로 진입
  - clone은 tree write lock 사용
  - write 대상 노드는 fs_refcount 기반으로 COW 여부 결정
  - COW로 주소가 바뀌면 부모 child pointer를 즉시 갱신
  - seq odd/even read snapshot, CAS publish 단계는 사용하지 않음

## 13) cow_bt (ref3_bt-style port, current)
- Result (100K test):
  - 1T 9474.72 -> 2T 7879.80 -> 4T 4227.85 -> 8T 2134.86 ops/sec
  - insert_lock_wait_avg_us: 0.03 -> 0.10 수준으로 낮게 유지
  - page_appends: 299,028 -> 544,380 -> 1,143,354 -> 2,285,170 (스레드 증가 시 급증)
  - publish_retries: 0 -> 65,777 -> 219,070 -> 510,787 (고스레드에서 급증)
  - cow_shared/cow_unique가 함께 관측되어 refcount 기반 COW 분기 자체는 동작
- Interpretation:
  - 현재 구현은 optimistic publish(CAS 유사) 구조라서 스레드가 같은 snapshot에서
    동시에 트리를 만들고, publish 단계에서 충돌한 쓰레드는 전체 insert를 재시도한다.
  - 따라서 lock wait 자체는 낮아도(락 대기 아님), retry로 인한 중복 COW/append 작업이
    누적되어 page_appends와 실행 시간이 함께 증가한다.
  - 병목은 append 장치 지연보다 retry로 인한 write amplification/CPU 낭비 성격이 강함.

## 14) ref1_mrallc (refs/ref1_mrallc)
- 코드/문서 단서:
  - README에서 "asynchronous" + "copy-on-write immutable root"를 명시
  - API가 `add(path, value, cb)`, `scan(root, ...)`, `get(root, ...)` 형태로 루트 ID를 인자로 전달
  - COW는 `cow(path, node, cb)`에서 부모를 재작성해 새 루트 ID를 만들어 반환
- 동시성/락 관점 정리:
  - 내부에 pthread mutex/rwlock/CAS/atomic 시퀀스 제어는 없음
  - Node.js 단일 이벤트 루프(콜백 비동기) 모델 전제라, 동일 프로세스 내 병렬 write 경쟁을 락으로 제어하지 않음
  - "snapshot"은 전역 seq가 아니라, 호출자가 들고 있는 root ID 자체가 버전 역할
  - 즉, 멀티스레드 동시성 제어 구현이라기보다 "버전 루트 전달형 immutable COW" 모델

## 15) ref2_boisterous (refs/ref2_boisterous, OceanBase COW btree 포크)
- 코드 단서(동시성 관련):
  - `BtreeBase::get_read_handle`에서 `root_pointer_`를 잡고 `refcas`로 refcount 증가(CAS)
  - `BtreeWriteHandle::start/end`에서 `pthread_mutex_lock/unlock`로 write 구간 보호
  - `BtreeBase::callback_done`에서 새 root publish 후 old root를 recycle list로 이동, refcount/sequence 기반 회수
  - `KeyBtree` 생성자에서 기본적으로 `set_write_lock_enable(true)` 적용
- 동시성/락 관점 정리:
  - Read: lock-free에 가까운 read snapshot 핸들(`root_pointer_` + refcount CAS)
  - Write: 기본 설정에서 전역 write mutex 직렬화(쓰기 핸들 획득~종료까지 보호)
  - Publish: optimistic retry(CAS publish loop) 방식이 아니라, write lock 하에서 root pointer 교체
  - GC/회수: root sequence + refcount(CAS) 조합으로 아직 참조 중인 세대는 남기고, 안전한 세대만 node 해제
  - 결론적으로 "read는 snapshot/CAS, write는 mutex 직렬화" 혼합 모델

## 16) ref5_zfs (refs/ref5_zfs, OpenZFS reference)
- 핵심 구조 요약:
  - txg(open -> quiescing -> syncing) 파이프라인으로 write를 그룹 커밋
  - write path는 dmu_tx assign/commit 후 sync 단계에서 실제 반영
  - sync context는 일관성 지점에서 트리를 갱신하고 uberblock을 publish
  - 백그라운드 flush/sync 작업과 호출자 경로를 분리해 내구성/처리량 균형을 맞춤
- 동시성 관점:
  - 단일 전역 락 모델이 아니라 역할별 lock/refcount/taskq 조합
  - read는 스냅샷 관점 유지, write는 txg 경계에서 정합성 확보

## 17) cow_zfs (ref5_zfs-inspired, current)
- 구현 요약:
  - cow_ram 계열 single root COW를 유지하면서 txg 스타일 비동기 반영 구조 도입
  - insert 요청 큐 -> batch/sort -> overlay apply -> flush -> publish 단계로 처리
  - durable superblock 반영은 별도 flusher thread가 주기적으로 수행
- 최근 튜닝 포인트:
  - 2-stage pipeline(txg_batch + txg_commit) 적용
  - stage1 과대 drain 시 avg_batch≈1 고정 문제가 있어 coalescing 정책 조정
  - commit 단계 opportunistic merge를 추가해 고스레드에서 batch 효율 개선
- 1M 기준 관찰(최근 로그):
  - 1T~2T는 cow_ram 대비 낮음(단건/저병렬 지연 불리)
  - 4T부터 유사/역전, 8T~64T는 cow_ram 대비 우세
  - 64T에서 약 16.9K~22.7K까지 관측(튜닝/러닝 조건에 따라 변동)
  - 즉, 저스레드 latency보다 고스레드 throughput 최적화 성격이 강함

## 18) cow_ram_stage2 (cow_ram + zfs-style stage2 pipeline)
- Result file: results/ram+stage2.txt
- Code files: cow_ram_stage2.c, cow_ram_stage2.h
- Applied:
  - zfs에서 사용한 2-stage 처리 아이디어를 ram 버전에 이식
  - stage1(sync): insert queue에서 batch drain + sort
  - stage2(commit): stage2 queue의 job들을 merge 후 overlay apply/flush/publish
  - low-thread 손해를 줄이기 위해, queue depth 기반 coalescing wait 조건을 둬서 저부하에서는 대기 최소화
  - 통계에 commit 관점 배치 효율(avg_commit_batch_size)을 추가해 stage1 평균과 분리 관찰 가능

- 1M throughput 비교(요약):
  - cow_ram: 1T 8946, 64T 15429 (results/ram.txt)
  - cow_zfs: 1T 5257, 64T 16940 (results/zfs_1M.txt)
  - cow_ram_stage2: 1T 8097, 64T 17805 (results/ram+stage2.txt)

- 해석:
  - 저스레드(1T~2T): cow_ram_stage2는 ram 대비 소폭 낮지만 zfs 대비는 높음
  - 중~고스레드(8T~64T): cow_ram_stage2가 ram을 확실히 앞서고, 64T에서는 zfs 대비도 우세
  - 즉 ram의 저지연 특성과 zfs식 stage2 병합 구조를 절충해, 고병렬 throughput을 끌어올린 버전으로 정리 가능