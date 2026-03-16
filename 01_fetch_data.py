import os
import csv
import random
from dataclasses import dataclass
from dotenv import load_dotenv
import psycopg2 as pg
import polars as pl

load_dotenv()

DATA_DIR = "data"
DIGITS_DIR = f"{DATA_DIR}/digits"


@dataclass
class OcrData:
    id: int
    panorama_id: str
    text: str
    ocr_yaw: float
    ocr_pitch: float
    ocr_width: float
    ocr_height: float
    lat: float
    lon: float
    heading: float
    pitch: float
    roll: float


def fetch_ocr_data():
    """Fetch OCR data from the database and save to CSV."""
    conn = pg.connect(os.getenv("DATABASE_URL"))

    query = """
    SELECT
        ocr.id,
        ocr.panorama_id,
        ocr.text,
        ocr.yaw AS "ocr_yaw",
        ocr.pitch AS "ocr_pitch",
        ocr.width AS "ocr_width",
        ocr.height AS "ocr_height",
        sv.lat,
        sv.lon,
        sv.heading,
        sv.pitch,
        sv.roll
    FROM
        "public"."ocr_result" AS ocr
    JOIN
        "public"."streetview" AS sv ON ocr.panorama_id = sv.panorama_id
    WHERE
        ocr.text ~ '^\\d{1,4}$'
        AND ocr.text::int BETWEEN 0 AND 2500
        AND ocr.confidence > 0.9;
    """

    print("Querying database...")
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    os.makedirs(DATA_DIR, exist_ok=True)

    with open(f"{DATA_DIR}/ocr_data.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id", "panorama_id", "text",
            "ocr_yaw", "ocr_pitch", "ocr_width", "ocr_height",
            "lat", "lon", "heading", "pitch", "roll",
        ])
        writer.writerows(results)

    print(f"Fetched {len(results)} rows → {DATA_DIR}/ocr_data.csv")
    conn.close()


def process_ocr_data():
    """Split ocr_data.csv into per-digit CSV files."""
    df = pl.read_csv(f"{DATA_DIR}/ocr_data.csv", schema_overrides={"text": pl.Utf8})
    unique_texts = df["text"].unique()
    print(f"{len(unique_texts)} unique texts found")

    os.makedirs(DIGITS_DIR, exist_ok=True)

    float_cols = [
        col for col, dtype in zip(df.columns, df.dtypes)
        if dtype == pl.Float64
    ]
    df = df.with_columns(
        [pl.col(col).cast(pl.Float32).round(2) for col in float_cols]
    )

    for text in unique_texts:
        df_text = df.filter(pl.col("text") == text)
        df_text.write_csv(f"{DIGITS_DIR}/{text}.csv")

    print(f"Digit files written to {DIGITS_DIR}/")


def get_sample_url():
    """Pick a random digit file and row, print a Google Street View URL."""
    import utils

    files = os.listdir(DIGITS_DIR)
    random_file = random.choice(files)
    print("picked file:", random_file)

    ocr_data = []
    with open(f"{DIGITS_DIR}/{random_file}", "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            ocr_data.append(OcrData(*row))

    random_ocr_data = random.choice(ocr_data)
    gsv_prop = utils.get_google_streetview_props(
        random_ocr_data.panorama_id,
        float(random_ocr_data.lat),
        float(random_ocr_data.lon),
        float(random_ocr_data.ocr_yaw),
        float(random_ocr_data.ocr_pitch),
        float(random_ocr_data.heading),
        float(random_ocr_data.pitch),
        float(random_ocr_data.roll),
        float(random_ocr_data.ocr_width),
        float(random_ocr_data.ocr_height),
    )
    url = utils.get_google_streetview_url(gsv_prop)
    print(url)


def get_stats():
    """Print digit files sorted by fewest sightings."""
    files = os.listdir(DIGITS_DIR)
    stats = {}
    for file in files:
        path = f"{DIGITS_DIR}/{file}"
        with open(path, "r") as f:
            stats[file] = sum(1 for _ in f)

    sorted_stats = sorted(stats.items(), key=lambda x: x[1])
    print(sorted_stats[:100])


if __name__ == "__main__":
    fetch_ocr_data()
    process_ocr_data()
    get_stats()
