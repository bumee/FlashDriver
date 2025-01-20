import pandas as pd
import matplotlib.pyplot as plt
import os

# 데이터 로드
data1 = pd.read_csv(os.getcwd() + "/compressed_throughput.csv")  # 기존 데이터
data2 = pd.read_csv(os.getcwd() + "/non_compressed_throughput.csv")  # 새로운 데이터

# 플롯
plt.plot(data1["x"], label="LZ4 Compression", color="blue")
plt.plot(data2["x"], label="Non Compression", color="red")

# 그래프 제목 및 축 레이블
plt.title("Comparison of SSD Throughput")
plt.xlabel("number of trials")
plt.ylabel("Throughput (mb/s)")
plt.legend()
plt.grid(True)

# 결과 저장
plt.savefig("comparison_throughput.png")
print("Comparison plot saved to comparison_throughput.png")
