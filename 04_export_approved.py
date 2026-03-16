import sqlite3
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(script_dir, "data", "process.db")
output_dir = os.path.join(script_dir, "public")
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "street_time_approved.db")

# Remove existing export db
if os.path.exists(output_path):
    os.remove(output_path)

# Read from process db
src = sqlite3.connect(db_path)
src.row_factory = sqlite3.Row
cursor = src.cursor()
cursor.execute("SELECT * FROM panoramas WHERE approved IN ('auto_approved', 'manual_approved') ORDER BY text ASC")
rows = cursor.fetchall()
src.close()

# Write to export db (keep id as primary key, omit approved field)
dst = sqlite3.connect(output_path)
dst.execute("""
    CREATE TABLE panoramas (
        id INTEGER PRIMARY KEY,
        panorama_id TEXT,
        text INTEGER,
        ocr_yaw REAL,
        ocr_pitch REAL,
        ocr_width REAL,
        ocr_height REAL,
        lat REAL,
        lon REAL,
        heading REAL,
        pitch REAL,
        roll REAL
    )
""")

dst.executemany(
    "INSERT INTO panoramas VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
    [(r["id"], r["panorama_id"], r["text"], r["ocr_yaw"], r["ocr_pitch"],
      r["ocr_width"], r["ocr_height"], r["lat"], r["lon"],
      r["heading"], r["pitch"], r["roll"]) for r in rows]
)

dst.execute("CREATE INDEX idx_text ON panoramas(text)")
dst.commit()
dst.close()

print(f"Exported {len(rows)} approved rows to {output_path}")
