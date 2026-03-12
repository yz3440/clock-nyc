import sqlite3
import os
from collections import defaultdict

THRESHOLD = 20

db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "process.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Query ALL rows (no filtering in SQL)
cursor.execute("SELECT * FROM panoramas")
all_rows = cursor.fetchall()
conn.close()

# Count approved rows per time in Python
approved_counts = defaultdict(int)
for row in all_rows:
    time_text = str(row[2])  # text column
    approved = row[12]       # approved column
    if approved:
        approved_counts[time_text] += 1

# Generate all possible times (0-23 hours, 00-59 minutes)
all_times = [f"{h}{m:02d}" for h in range(24) for m in range(60)]

fully_covered = []
not_fully_covered = []

for t in all_times:
    count = approved_counts.get(t, 0)
    if count >= THRESHOLD:
        fully_covered.append((t, count))
    else:
        not_fully_covered.append((t, count))

print(f"=== FULLY COVERED (>={THRESHOLD} approved): {len(fully_covered)} times ===")
for t, count in fully_covered:
    print(f"  {t}  ({count} approved)")

print()
print(f"=== NOT FULLY COVERED (<{THRESHOLD} approved): {len(not_fully_covered)} times ===")
for t, count in not_fully_covered:
    print(f"  {t}  ({count} approved)")

# Find contiguous blocks of not-fully-covered times
not_covered_set = set(t for t, _ in not_fully_covered)
blocks = []
block_start = None
prev_time = None

for t in all_times:
    if t in not_covered_set:
        if block_start is None:
            block_start = t
        prev_time = t
    else:
        if block_start is not None:
            blocks.append((block_start, prev_time))
            block_start = None
            prev_time = None

if block_start is not None:
    blocks.append((block_start, prev_time))

print()
print(f"=== BLOCKS STILL NEEDED: {len(blocks)} ===")
for start, end in blocks:
    if start == end:
        print(f"  {start}")
    else:
        print(f"  {start} - {end}")

print()
print(f"Total: {len(fully_covered)} fully covered, {len(not_fully_covered)} not fully covered out of {len(all_times)} times")
