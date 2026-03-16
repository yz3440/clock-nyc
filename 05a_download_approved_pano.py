"""Download panoramas for approved entries to .pano_cache/"""
import asyncio
import os
import sqlite3

import aiohttp
from streetlevel import streetview
from tqdm import tqdm

DIR = os.path.dirname(os.path.abspath(__file__))
APPROVED_DB_PATH = os.path.join(DIR, "public", "street_time_approved.db")
CORRECTED_DB_PATH = os.path.join(DIR, "public", "street_time_corrected.db")
PANO_CACHE_DIR = os.path.join(DIR, ".pano_cache")

CONCURRENCY = 8
ZOOM_LEVEL = 5

os.makedirs(PANO_CACHE_DIR, exist_ok=True)


def _table_has_column(conn, table, column):
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    return column in cols


def init_corrected_db():
    """Create the corrected db with the same panoramas schema as approved, plus a missing_panoramas table."""
    conn = sqlite3.connect(CORRECTED_DB_PATH)

    # Check if existing tables have old schema (missing id column) and recreate
    existing_tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "panoramas" in existing_tables and not _table_has_column(conn, "panoramas", "id"):
        print("Detected old schema without id column — dropping and recreating tables.")
        conn.execute("DROP TABLE IF EXISTS panoramas")
        conn.execute("DROP TABLE IF EXISTS text_not_found_panoramas")
        conn.execute("DROP INDEX IF EXISTS idx_text")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS panoramas (
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_text ON panoramas(text)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS missing_panoramas (
            panorama_id TEXT PRIMARY KEY
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS text_not_found_panoramas (
            id INTEGER PRIMARY KEY,
            panorama_id TEXT,
            text INTEGER
        )
    """)
    conn.commit()
    return conn


def cache_path(panorama_id: str) -> str:
    return os.path.join(PANO_CACHE_DIR, f"{panorama_id}.jpg")


async def download_one(session: aiohttp.ClientSession, panorama_id: str, missing: set) -> tuple[str, bool]:
    """Returns (panorama_id, success)."""
    path = cache_path(panorama_id)
    if os.path.exists(path):
        return panorama_id, True

    try:
        pano = await streetview.find_panorama_by_id_async(panorama_id, session)
        if pano is None:
            missing.add(panorama_id)
            return panorama_id, False

        await streetview.download_panorama_async(pano, path, session, zoom=ZOOM_LEVEL)
        return panorama_id, True
    except Exception as e:
        print(f"  Error downloading {panorama_id}: {e}")
        missing.add(panorama_id)
        return panorama_id, False


async def main():
    corrected_conn = init_corrected_db()

    # Load already-known missing panoramas
    known_missing = {row[0] for row in corrected_conn.execute("SELECT panorama_id FROM missing_panoramas").fetchall()}

    # Load all processed ids from corrected + text_not_found
    processed_ids = {
        r[0] for r in corrected_conn.execute("SELECT id FROM panoramas").fetchall()
    } | {
        r[0] for r in corrected_conn.execute("SELECT id FROM text_not_found_panoramas").fetchall()
    }

    # Get all (id, panorama_id) from approved db, grouped by panorama_id
    approved_conn = sqlite3.connect(APPROVED_DB_PATH)
    approved_entries = approved_conn.execute("SELECT id, panorama_id FROM panoramas").fetchall()
    approved_conn.close()

    # Group by panorama_id and find which still have pending entries
    pano_entries: dict[str, list] = {}
    for row_id, pid in approved_entries:
        pano_entries.setdefault(pid, []).append(row_id)

    # A panorama needs downloading only if it has unprocessed entries
    needs_download = set()
    for pid, ids in pano_entries.items():
        if any(i not in processed_ids for i in ids):
            needs_download.add(pid)

    to_download = [
        pid for pid in needs_download
        if pid not in known_missing
        and not os.path.exists(cache_path(pid))
    ]

    # Entry-level progress
    total_entries = len(approved_entries)
    missing_entry_count = sum(len(ids) for pid, ids in pano_entries.items() if pid in known_missing)
    processed_entry_count = len(processed_ids) + missing_entry_count
    print(f"\nEntry progress: {processed_entry_count}/{total_entries} "
          f"(corrected: {len(processed_ids)}, missing pano: {missing_entry_count})")

    # Panorama-level progress
    already = len(pano_entries) - len(to_download)
    print(f"Panorama progress: {already}/{len(pano_entries)} "
          f"(cached/processed/missing), {len(to_download)} to download")

    if not to_download:
        print("Nothing to download.")
        corrected_conn.close()
        return

    sem = asyncio.Semaphore(CONCURRENCY)
    downloaded = 0
    new_missing = set()

    async with aiohttp.ClientSession() as session:
        async def fetch(pid):
            async with sem:
                return await download_one(session, pid, new_missing)

        pbar = tqdm(total=len(to_download), desc="Downloading")
        try:
            tasks = [fetch(pid) for pid in to_download]
            for coro in asyncio.as_completed(tasks):
                pid, success = await coro
                if success:
                    downloaded += 1
                pbar.update(1)
        except KeyboardInterrupt:
            print("\nInterrupted.")
        finally:
            pbar.close()

    # Persist newly discovered missing panoramas
    if new_missing:
        corrected_conn.executemany(
            "INSERT OR IGNORE INTO missing_panoramas (panorama_id) VALUES (?)",
            [(pid,) for pid in new_missing],
        )
        corrected_conn.commit()

    corrected_conn.close()
    print(f"\nDownloaded {downloaded}. {len(known_missing) + len(new_missing)} total missing.")


if __name__ == "__main__":
    asyncio.run(main())
