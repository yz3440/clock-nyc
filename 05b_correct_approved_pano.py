"""Correct OCR coordinates using cached panoramas from .pano_cache/"""
import argparse
import json
import os
import random
import sqlite3
import time

from PIL import Image
Image.MAX_IMAGE_PIXELS = None

from tqdm import tqdm
from panoocr.geometry import perspective_to_sphere
from panoocr.image.models import PanoramaImage, PerspectiveMetadata
from panoocr.engines.macocr import (
    MacOCREngine,
    MacOCRLanguageCode,
    MacOCRRecognitionLevel,
)

DIR = os.path.dirname(os.path.abspath(__file__))
APPROVED_DB_PATH = os.path.join(DIR, "public", "street_time_approved.db")
CORRECTED_DB_PATH = os.path.join(DIR, "public", "street_time_corrected.db")
PANO_CACHE_DIR = os.path.join(DIR, ".pano_cache")
PERSPECTIVES_DIR = os.path.join(DIR, ".perspectives")

FOV_MULTIPLIERS = [5.0, 10.0, 15.0]
RESOLUTION = 2048


def cache_path(panorama_id: str) -> str:
    return os.path.join(PANO_CACHE_DIR, f"{panorama_id}.jpg")


def normalize_yaw(yaw: float) -> float:
    while yaw > 180:
        yaw -= 360
    while yaw < -180:
        yaw += 360
    return yaw


def find_text_matches(ocr_results: list[dict], target_text: str) -> list[dict]:
    matches = []
    for result in ocr_results:
        text = result.get("text", "").strip()
        if text.upper() == target_text.upper():
            matches.append(result)
    return matches


def find_closest_to_center(matches: list[dict], size: int) -> dict | None:
    if not matches:
        return None
    center = size / 2

    def dist(m):
        bbox = m.get("bbox", [0, 0, 0, 0])
        mx = (bbox[0] + bbox[2]) / 2
        my = (bbox[1] + bbox[3]) / 2
        return ((mx - center) ** 2 + (my - center) ** 2) ** 0.5

    return min(matches, key=dist)


def calibrate_entry(pano_path: str, panorama_id: str, target_text: str,
                    yaw: float, pitch: float,
                    ocr_width: float, ocr_height: float,
                    engine: MacOCREngine,
                    save_perspectives: bool = False) -> dict | None:
    """Re-run OCR on perspective crops and return corrected coordinates, or None."""
    yaw_offset = normalize_yaw(yaw)
    pitch_offset = pitch

    base_size = max(ocr_width, ocr_height)
    fovs_to_try = [base_size * m for m in FOV_MULTIPLIERS]

    last_perspective_pil = None
    last_fov = None
    last_ocr_dicts = None

    for fov in fovs_to_try:
        meta = PerspectiveMetadata(
            pixel_width=RESOLUTION,
            pixel_height=RESOLUTION,
            horizontal_fov=fov,
            vertical_fov=fov,
            yaw_offset=yaw_offset,
            pitch_offset=pitch_offset,
        )

        try:
            panorama = PanoramaImage(panorama_id, pano_path)
            perspective = panorama.generate_perspective_image(meta)
            perspective_pil = perspective.get_perspective_image()
        except Exception:
            continue

        last_perspective_pil = perspective_pil
        last_fov = fov

        try:
            ocr_results = engine.recognize(perspective_pil)
        except Exception:
            continue

        ocr_dicts = []
        for r in ocr_results:
            bbox = r.bounding_box
            ocr_dicts.append({
                "text": r.text,
                "confidence": r.confidence,
                "bbox": [
                    bbox.left * RESOLUTION,
                    bbox.top * RESOLUTION,
                    bbox.right * RESOLUTION,
                    bbox.bottom * RESOLUTION,
                ],
            })

        last_ocr_dicts = ocr_dicts

        matches = find_text_matches(ocr_dicts, target_text)
        if not matches:
            continue

        best = find_closest_to_center(matches, RESOLUTION)
        if not best:
            continue

        if best.get("confidence", 1.0) < 0.69:
            continue

        bbox = best["bbox"]
        u = (bbox[0] + bbox[2]) / 2 / RESOLUTION
        v = (bbox[1] + bbox[3]) / 2 / RESOLUTION
        box_w = (bbox[2] - bbox[0]) / RESOLUTION
        box_h = (bbox[3] - bbox[1]) / RESOLUTION

        world_yaw, world_pitch = perspective_to_sphere(
            u=u, v=v,
            horizontal_fov=fov, vertical_fov=fov,
            yaw_offset=yaw_offset, pitch_offset=pitch_offset,
        )

        if save_perspectives and perspective_pil:
            _save_perspective(perspective_pil, panorama_id, fov, "corrected", ocr_dicts)

        return {
            "ocr_yaw": round(world_yaw, 2),
            "ocr_pitch": round(world_pitch, 2),
            "ocr_width": round(box_w * fov, 2),
            "ocr_height": round(box_h * fov, 2),
        }

    if save_perspectives and last_perspective_pil:
        _save_perspective(last_perspective_pil, panorama_id, last_fov, "failed", last_ocr_dicts)

    return None


def _save_perspective(img, panorama_id, fov, status, ocr_dicts):
    """Save a perspective image and its OCR results for debugging."""
    os.makedirs(PERSPECTIVES_DIR, exist_ok=True)
    prefix = f"{panorama_id}_fov{int(fov)}_{status}"
    img.save(os.path.join(PERSPECTIVES_DIR, f"{prefix}.jpg"), "JPEG", quality=90)
    if ocr_dicts:
        with open(os.path.join(PERSPECTIVES_DIR, f"{prefix}_ocr.json"), "w") as f:
            json.dump(ocr_dicts, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--save-perspectives", action="store_true",
                        help="Save perspective crops and OCR results to .perspectives/")
    parser.add_argument("--no-delete-after", action="store_true",
                        help="Keep panorama in cache after processing (default: delete)")
    args = parser.parse_args()

    engine = MacOCREngine(config={
        "language_preference": [MacOCRLanguageCode.ENGLISH_US],
        "recognition_level": MacOCRRecognitionLevel.ACCURATE,
    })

    # Read source rows from approved db
    approved_conn = sqlite3.connect(APPROVED_DB_PATH)
    approved_conn.row_factory = sqlite3.Row
    all_rows = [dict(r) for r in approved_conn.execute("SELECT * FROM panoramas").fetchall()]
    approved_count = len(all_rows)
    approved_conn.close()

    # Open corrected db (created by 05a)
    corrected_conn = sqlite3.connect(CORRECTED_DB_PATH, timeout=30)
    corrected_conn.execute("PRAGMA journal_mode=WAL")
    corrected_conn.commit()

    corrected_total = 0
    failed_total = 0
    MAX_EMPTY_ROUNDS = 5
    empty_rounds = 0

    try:
        while empty_rounds < MAX_EMPTY_ROUNDS:
            # Reload processed state each round
            already_done_ids = {
                r[0] for r in corrected_conn.execute("SELECT id FROM panoramas").fetchall()
            } | {
                r[0] for r in corrected_conn.execute("SELECT id FROM text_not_found_panoramas").fetchall()
            }
            missing_panos = {r[0] for r in corrected_conn.execute("SELECT panorama_id FROM missing_panoramas").fetchall()}

            # Count entries covered by missing panoramas
            missing_entry_count = sum(
                1 for r in all_rows if r["panorama_id"] in missing_panos
            )
            processed_count = len(already_done_ids) + missing_entry_count

            print(f"\nProgress: {processed_count}/{approved_count} "
                  f"(corrected: {len(already_done_ids)}, "
                  f"missing pano: {missing_entry_count})")

            if processed_count >= approved_count:
                print("All entries accounted for.")
                break

            # Find pending rows
            pending = [
                r for r in all_rows
                if r["id"] not in already_done_ids
                and r["panorama_id"] not in missing_panos
            ]

            # Group by panorama_id, find ready ones
            groups: dict[str, list] = {}
            for row in pending:
                groups.setdefault(row["panorama_id"], []).append(row)

            ready_groups = {pid: rows for pid, rows in groups.items() if os.path.exists(cache_path(pid))}
            ready_list = list(ready_groups.items())
            random.shuffle(ready_list)
            ready_count = sum(len(rows) for rows in ready_groups.values())

            if not ready_count:
                empty_rounds += 1
                waiting = len(pending)
                if empty_rounds < MAX_EMPTY_ROUNDS:
                    print(f"No cached panoramas ready ({waiting} entries waiting). "
                          f"Waiting 5s... ({empty_rounds}/{MAX_EMPTY_ROUNDS})")
                    time.sleep(5)
                continue

            empty_rounds = 0
            pbar = tqdm(total=ready_count, desc="Correcting OCR")

            for pid, group_rows in ready_list:
                path = cache_path(pid)

                for row in group_rows:
                    calibration = calibrate_entry(
                        pano_path=path,
                        panorama_id=pid,
                        target_text=str(row["text"]),
                        yaw=row["ocr_yaw"],
                        pitch=row["ocr_pitch"],
                        ocr_width=row["ocr_width"],
                        ocr_height=row["ocr_height"],
                        engine=engine,
                        save_perspectives=args.save_perspectives,
                    )

                    if calibration is not None:
                        corrected_conn.execute(
                            "INSERT OR IGNORE INTO panoramas VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                            (row["id"], pid, row["text"],
                             calibration["ocr_yaw"], calibration["ocr_pitch"],
                             calibration["ocr_width"], calibration["ocr_height"],
                             row["lat"], row["lon"],
                             row["heading"], row["pitch"], row["roll"]),
                        )
                        corrected_total += 1
                    else:
                        corrected_conn.execute(
                            "INSERT OR IGNORE INTO text_not_found_panoramas VALUES (?,?,?)",
                            (row["id"], pid, row["text"]),
                        )
                        failed_total += 1

                    pbar.update(1)

                corrected_conn.commit()

                if not args.no_delete_after:
                    try:
                        os.remove(path)
                    except OSError:
                        pass

            pbar.close()

    except KeyboardInterrupt:
        print("\nInterrupted. Saving progress...")
        corrected_conn.commit()

    corrected_conn.close()
    print(f"\nDone. Corrected: {corrected_total}, OCR failed: {failed_total}")
    print(f"Output: {CORRECTED_DB_PATH}")


if __name__ == "__main__":
    main()
