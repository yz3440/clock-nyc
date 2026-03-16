import pandas as pd
import sqlite3
import glob
import os

DATA_DIR = "data"
DIGITS_DIR = f"{DATA_DIR}/digits"
DB_PATH = f"{DATA_DIR}/process.db"


def create_sqlite_db():
    """Convert digit CSV files into a SQLite database."""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed existing {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)

    try:
        csv_files = glob.glob(os.path.join(DIGITS_DIR, '*.csv'))
        print(f"Found {len(csv_files)} CSV files")

        for csv_file in csv_files:
            df = pd.read_csv(csv_file)
            df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
            df.to_sql('panoramas', conn, if_exists='append', index=False)

        cursor = conn.cursor()
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_id ON panoramas(id);')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_lat_lon ON panoramas(lat, lon);')
        cursor.execute('ALTER TABLE panoramas ADD approved TEXT DEFAULT NULL')

        cursor.execute('SELECT COUNT(*) FROM panoramas;')
        total_rows = cursor.fetchone()[0]
        print(f"Database created: {DB_PATH}")
        print(f"Total records: {total_rows}")

    finally:
        conn.close()


if __name__ == "__main__":
    create_sqlite_db()
