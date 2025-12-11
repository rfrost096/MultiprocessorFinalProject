import matplotlib.pyplot as plt

# Scalability test
# params: -f datasets/balanced.txt -s -r -b 64
# storing three tests for averaging

lock_based: dict[int, list[float]] = {
    1: [0.146073, 0.157388, 0.178463],
    2: [0.075207, 0.087718, 0.087677],
    4: [0.051254, 0.054355, 0.049573],
    8: [0.041970, 0.043972, 0.041221],
    12: [0.039524, 0.040486, 0.044179]
}

lock_free: dict[int, list[float]] = {
    1: [0.169666, 0.160903, 0.177773],
    2: [0.067601, 0.078381, 0.084433],
    4: [0.040757, 0.044748, 0.050734],
    8: [0.025231, 0.026030, 0.029643],
    12: [0.020514, 0.022841, 0.022515]
}

thread = [1, 2, 4, 8, 12]
lock_based_performance = [sum(lock_based[i])/len(lock_based[i]) for i in thread]
lock_free_performance = [sum(lock_free[i])/len(lock_free[i]) for i in thread]

plt.plot(thread, lock_based_performance, label="Lock-Based")
plt.plot(thread, lock_free_performance, label="Lock-Free")

plt.yscale('log')

plt.grid(True, which="major", linestyle='-', linewidth=0.7, alpha=0.7)
plt.grid(True, which="minor", linestyle='--', linewidth=0.5, alpha=0.5)

plt.xlabel("Number of Threads")
plt.ylabel("Execution time (s)")
plt.title("Scalability test")

plt.legend()

plt.show()

# Now allowing resizing
# params: -f datasets/balanced.txt -s -b 64

lock_based: dict[int, list[float]] = {
    1: [0.088141, 0.098156, 0.090600],
    2: [0.021406, 0.015361, 0.020208],
    4: [0.011200, 0.010167, 0.012835],
    8: [0.013712, 0.011744, 0.011993],
    12: [0.013002, 0.012696, 0.012987]
}

lock_free: dict[int, list[float]] = {
    1: [0.085284, 0.083793, 0.092738],
    2: [0.013151, 0.016928, 0.016829],
    4: [0.009446, 0.011930, 0.014565],
    8: [0.011499, 0.010268, 0.013963],
    12: [0.012413, 0.012950, 0.013906]
}

thread = [1, 2, 4, 8, 12]
lock_based_performance = [sum(lock_based[i])/len(lock_based[i]) for i in thread]
lock_free_performance = [sum(lock_free[i])/len(lock_free[i]) for i in thread]

plt.plot(thread, lock_based_performance, label="Lock-Based")
plt.plot(thread, lock_free_performance, label="Lock-Free")

plt.yscale('log')

plt.grid(True, which="major", linestyle='-', linewidth=0.7, alpha=0.7)
plt.grid(True, which="minor", linestyle='--', linewidth=0.5, alpha=0.5)

plt.xlabel("Number of Threads")
plt.ylabel("Execution time (s)")
plt.title("Scalability Test w/ Resizing")

plt.legend()

plt.show()

# params: -f datasets/write_heavy.txt -t 1 -s -b 64 -r
# params: -f datasets/read_heavy.txt -t 1 -s -b 64 -r

lock_based_write: dict[int, list[float]] = {
    1: [0.387829, 0.392013, 0.421767],
    2: [0.199604, 0.198094, 0.197993],
    4: [0.123234, 0.141118, 0.130646],
    8: [0.109804, 0.106991, 0.101883],
    12: [0.106735, 0.091574, 0.092298]
}

lock_free_write: dict[int, list[float]] = {
    1: [0.431276, 0.387798, 0.406572],
    2: [0.173719, 0.172825, 0.198105],
    4: [0.108099, 0.108624, 0.110324],
    8: [0.071704, 0.062911, 0.061903],
    12: [0.054194, 0.049006, 0.047574]
}

lock_based_read: dict[int, list[float]] = {
    1: [0.039134, 0.042416, 0.033547],
    2: [0.020335, 0.018273, 0.018288],
    4: [0.013414, 0.012297, 0.012696],
    8: [0.010276, 0.009843, 0.008801],
    12: [0.010898, 0.010473, 0.008772]
}

lock_free_read: dict[int, list[float]] = {
    1: [0.040809, 0.039818, 0.036643],
    2: [0.019939, 0.018984, 0.017304],
    4: [0.010376, 0.011524, 0.009769],
    8: [0.006499, 0.006893, 0.006100],
    12: [0.005472, 0.006940, 0.006969]
}

thread = [1, 2, 4, 8, 12]
lock_based_write_performance = [sum(lock_based_write[i])/len(lock_based_write[i]) for i in thread]
lock_free_write_performance = [sum(lock_free_write[i])/len(lock_free_write[i]) for i in thread]
lock_based_read_performance = [sum(lock_based_read[i])/len(lock_based_read[i]) for i in thread]
lock_free_read_performance = [sum(lock_free_read[i])/len(lock_free_read[i]) for i in thread]

plt.plot(thread, lock_based_write_performance, label="Lock-Based Write-Heavy")
plt.plot(thread, lock_free_write_performance, label="Lock-Free Write-Heavy")
plt.plot(thread, lock_based_read_performance, label="Lock-Based Read-Heavy")
plt.plot(thread, lock_free_read_performance, label="Lock-Free Read-Heavy")

plt.yscale('log')

plt.grid(True, which="major", linestyle='-', linewidth=0.7, alpha=0.7)
plt.grid(True, which="minor", linestyle='--', linewidth=0.5, alpha=0.5)

plt.xlabel("Number of Threads")
plt.ylabel("Execution time (s)")
plt.title("Scalability Test w/ Resizing")

plt.legend()

plt.show()