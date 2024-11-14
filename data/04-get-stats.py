# randomly select a file in "digits" dir
import os
import random
import csv
from dataclasses import dataclass

files = os.listdir("../digits")

stats = {}


for file in files:
    path = f"../digits/{file}"
    with open(path, "r") as f:
        lineCount = sum(1 for _ in f)
        stats[file] = lineCount

# find the top 10 files with the least number of lines
sorted_stats = sorted(stats.items(), key=lambda x: x[1])
print(sorted_stats[:100])
