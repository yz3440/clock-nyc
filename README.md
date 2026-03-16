# NYC Street Clock

NYC Street Clock is an online clock made out of over 20,000 Google Street View captures from around New York City that reflect the given time. In watching the minutes tick by and clicking around the different matches within a given moment, viewers see not only the passage of time but also the breadth of ways that numbers exist in the city: The M116 bus in the early afternoon; an advertisement for $6.50 halal food in the evening; or the awning of the jewelry store at 1204 Broadway past midnight.

## Pipeline

1. **`01_fetch_data.py`** — Pull OCR data from the proprietary [all text in nyc](https://alltext.nyc) database (PostgreSQL) into CSVs (the relevant subset is committed in `data/digits/`)
2. **`02_create_db.py`** — Build local SQLite from CSVs
3. **`03_classify_auto.py`** — Auto-classify with GPT vision
4. **`03_classify_manual.py`** — Manual classify with keyboard
5. **`04_export_approved.py`** — Export approved entries to `public/`
6. **`05a_download_approved_pano.py`** — Download approved panoramas to `.pano_cache/`, track missing IDs
7. **`05b_correct_approved_pano.py`** — Re-run OCR on cached panoramas at multiple FOVs to correct coordinates into `street_time_corrected.db`

## Setup

```sh
uv sync
cp .env.example .env                    # pipeline API keys
cp public/env.example.js public/env.js  # frontend Google Maps API key
```

Requires: PostgreSQL (for step 1), Google Maps API key, OpenAI API key.

## Deploy

The `public/` folder is a self-contained static site. Serve it with any static host.
