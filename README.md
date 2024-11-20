
## Setup

1. install requirements and put the postgres url in .env as `POSTGRES_URL`
2. run `create_db.py`
3. run `classifier_gui.py` and start classifying w/ "A" to reject and "L" to approve, you can just quit out when you're done

No back button yet :(

## basic db explanation

In order to work on this asynchronously, we use 2 databases:

1. `process.db` which is constructed from the CSVs by running `create_db.py`
2. a postgres database which serves as the source of truth for what has / has not been checked, which we connect to automatically in `classifier_gui.py`

When the script runs, we connect to the postgres db and then reference that to make sure we don't redundantly work on times / images we've already processed.