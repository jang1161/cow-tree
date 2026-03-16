import matplotlib.pyplot as plt

threads = [1, 2, 4, 8, 16, 32, 64]
x = range(len(threads))

ops_10k  = [7030, 9111, 11648, 13255, 15509, 18122, 20640]
ops_50k  = [4608, 6139, 7530, 7878, 8902, 10681, 12545]
ops_100k = [4291, 5730, 6303, 6803, 7113, 8480, 10094]
ops_1m   = [3645, 4322, 4855, 5152, 5403, 5615, 6231]

plt.figure(figsize=(8,6))

plt.plot(x, ops_10k, marker='o', label="10K keys")
plt.plot(x, ops_50k, marker='o', label="50K keys")
plt.plot(x, ops_100k, marker='o', label="100K keys")
plt.plot(x, ops_1m, marker='o', label="1M keys")

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