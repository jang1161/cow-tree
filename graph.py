import matplotlib.pyplot as plt

threads = [1, 2, 4, 8, 16, 32, 64]
x = range(len(threads))

ops_10k = [7761.74, 9224.92, 12467.90, 13691.89, 16534.86, 19787.97, 23254.82]
ops_100k = [8041.68, 9149.60, 11596.55, 12114.65, 14097.84, 17179.84, 20946.43]
ops_1m = [5128.76, 6004.18, 7002.98, 6858.33, 7417.41, 7793.52, 8862.18]
ops_10m = [3747.73, 4217.75, 3870.28, 4717.70, 5210.19, 5614.07, 5879.97]

plt.figure(figsize=(8,6))

plt.plot(x, ops_10k, marker='o', label="10K keys")
plt.plot(x, ops_100k, marker='o', label="100K keys")
plt.plot(x, ops_1m, marker='o', label="1M keys")
plt.plot(x, ops_10m, marker='o', label="10M keys")

plt.xlabel("Threads")
plt.ylabel("Ops/sec")
plt.title("Ops/sec vs Threads")

plt.xticks(x, threads)

# y축 0부터 시작
plt.ylim(bottom=0)

# 가로 grid만 표시
plt.grid(axis='y')

plt.legend()

plt.savefig("throughput_scaling.png", dpi=300, bbox_inches="tight")
plt.show()