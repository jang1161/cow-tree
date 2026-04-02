import matplotlib.pyplot as plt

# Data
threads = [1, 2, 4, 8, 16, 32, 64]
throughput = [2373, 4180, 6966, 11309, 15421, 18134, 20148]

# Plot
plt.figure(figsize=(9, 5))  # (가로, 세로)
plt.plot(threads, throughput, marker='o')

# Log scale for x-axis
plt.xscale('log', base=2)
plt.xticks(threads, threads)

# Labels
plt.xlabel('Threads')
plt.ylabel('Throughput (ops/sec)')
plt.title('1M / 4096 * 4 cache sets, 4 ways / batch flush')

plt.ylim(0, 65000)
plt.yticks([0, 20000, 40000, 60000])

# Grid
plt.grid(True, which="both", linestyle='--', linewidth=0.5)

# Save as PNG
plt.savefig('results/graphs/final_1M_60kScale2.png')
plt.show()