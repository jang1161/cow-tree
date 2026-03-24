import matplotlib.pyplot as plt
import numpy as np

threads = [1, 2, 4, 8, 16, 32, 64]
x = np.arange(len(threads))

# 데이터
ram = [8946, 10062, 11714, 12048, 12605, 13850, 15429]

zfs = [5257.01, 7228.26, 11808.38, 14041.69, 15170.87, 16121.80, 16940.40]

ram_stage2 = [8097.18, 10317.53, 12316.55, 13942.33, 15212.93, 16432.40, 17804.55]

# y축 범위 자동 설정
all_values = ram + zfs + ram_stage2
ymin = 0
ymax = max(all_values) * 1.1

plt.figure(figsize=(8, 5))

plt.plot(x, ram, marker='o', label='ram')
plt.plot(x, zfs, marker='o', label='zfs')
plt.plot(x, ram_stage2, marker='o', label='ram+stage2')

plt.xticks(x, threads)
plt.xlabel("Threads")
plt.ylabel("Throughput (ops/sec)")
plt.title("1M Keys Throughput Comparison")

plt.ylim(ymin, ymax)
plt.grid(True)
plt.legend()

plt.tight_layout()
plt.savefig("compare_1M_all.png")
plt.show()