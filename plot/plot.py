import pandas as pd
import matplotlib.pyplot as plt
import os

# 데이터 로드
data = pd.read_csv(os.getcwd()+"/data.csv")

# 플롯
plt.plot(data["x"], data["y"], label="latency cdf graph", color="blue")
plt.title("Plot of latency cdf")
plt.xlabel("x")
plt.ylabel("y")
plt.legend()
plt.grid(True)

plt.savefig("plot.png")
print("Plot saved to plot.png")
