import matplotlib.pyplot as plt

threads = [1, 2, 4, 8, 16, 32, 64]
x = range(len(threads))

ops_100k = [7953, 10274, 12222, 12880, 14265, 17374, 20820]
ops_1m   = [8946, 10062, 11714, 12048, 12605, 13850, 15429]
ops_10m  = [7434, 8218, 9600, 9890, 10892, 12148, 13287]

plt.figure(figsize=(8,6))

plt.plot(x, ops_100k, marker='o', label="100K keys")
plt.plot(x, ops_1m, marker='o', label="1M keys")
plt.plot(x, ops_10m, marker='o', label="10M keys")

plt.xlabel("Threads")
plt.ylabel("Ops")
plt.title("Ops vs Threads")

plt.xticks(x, threads)

# y축 0부터 시작
plt.ylim(bottom=0)

# 가로 grid만 표시
plt.grid(axis='y')

plt.legend()

plt.savefig("throughput_scaling.png", dpi=300, bbox_inches="tight")
plt.show()