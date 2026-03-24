import matplotlib.pyplot as plt
import numpy as np

threads = [1, 2, 4, 8, 16, 32, 64]
x = np.arange(len(threads))

# Version 1
v1_100k = [9240, 10355, 13360, 13664, 15258, 18223, 22250]
v1_1m   = [8793, 9868, 11874, 12263, 13535, 15865, 16584.42]
v1_10m  = [None, None, None, None, None, None, None]

# Version 2
v2_100k = [7740, 8449, 5506, 6443, 8609, 14360, 24002]
v2_1m   = [6693, 7907, 4998, 5595, 7883, 14498, 18003.14]
v2_10m  = [None, None, None, None, None, None, None]

all_values = [
    v for v in (
        v1_100k + v1_1m + v1_10m +
        v2_100k + v2_1m + v2_10m
    ) if v is not None
]

ymin = 0
ymax = max(all_values) * 1.1

fig, axs = plt.subplots(1, 3, figsize=(15, 4))

def plot_graph(ax, v1, v2, title):
    x1 = [i for i, v in enumerate(v1) if v is not None]
    y1 = [v for v in v1 if v is not None]
    if y1:
        ax.plot(x1, y1, marker='o', label='ram')

    x2 = [i for i, v in enumerate(v2) if v is not None]
    y2 = [v for v in v2 if v is not None]
    if y2:
        ax.plot(x2, y2, marker='o', label='ram_async')

    ax.set_title(title)
    ax.set_ylim(ymin, ymax)
    ax.set_xticks(x)
    ax.set_xticklabels(threads)

plot_graph(axs[0], v1_100k, v2_100k, "100K")
plot_graph(axs[1], v1_1m, v2_1m, "1M")
plot_graph(axs[2], v1_10m, v2_10m, "10M")

for ax in axs:
    ax.set_xlabel("Threads")
    ax.set_ylabel("Throughput")
    ax.grid(True)

axs[0].legend()

plt.tight_layout()
plt.savefig("compare_ram_async.png")
plt.show()