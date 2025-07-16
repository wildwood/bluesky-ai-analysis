import sys
import os
import logging
import glob
import duckdb
import datetime
import argparse

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info("Logger created.")


CONSOLIDATED_DIR = "consolidated"

def consolidate_day(export_dir: str, date_str: str):
    """
    Consolidate all chunk parquet files for a given date into a single file,
    skipping if the consolidated file is up to date.
    """
    pattern = os.path.join(export_dir, f"posts-{date_str}-*.parquet")
    chunk_files = glob.glob(pattern)
    if not chunk_files:
        logging.info(f"No chunk files for {date_str}, pattern {pattern}")
        return

    if not os.path.exists(os.path.join(export_dir, f"{CONSOLIDATED_DIR}")):
        logging.info(f"Target directory {CONSOLIDATED_DIR} does not exist")
        return

    consolidated_file = os.path.join(export_dir, f"{CONSOLIDATED_DIR}", f"posts-{date_str}.parquet")

    if os.path.exists(consolidated_file):
        consolidated_mtime = os.path.getmtime(consolidated_file)
        if all(os.path.getmtime(f) <= consolidated_mtime for f in chunk_files):
            logging.info(f"Skipping {date_str} (already consolidated and up to date)")
            return

    logging.info(f"Consolidating {len(chunk_files)} files for {date_str}")
    con = duckdb.connect()
    try:
        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{pattern}')
            ) TO '{consolidated_file}' (FORMAT 'parquet');
        """)
        logging.info(f"Consolidated to {consolidated_file}")
    except Exception as e:
        logging.info(f"Error consolidating for {date_str}: {e}")
    finally:
        con.close()

DATE_FORMAT_STRING = "%Y-%m-%d"

def get_date_strings(last_date, numDays):
    days = [last_date.strftime(DATE_FORMAT_STRING)]
    while len(days) < numDays:
        last_date -= datetime.timedelta(days=1)
        days.append(last_date.strftime(DATE_FORMAT_STRING))
    return days

CURRENT_DATE = datetime.date.today()
DAYS_BACK = 3
EXPORT_DIR = "."

# ----------------------------------------
# Parse command line
# ----------------------------------------
parser = argparse.ArgumentParser(
    description="Consolidate Bluesky embeddings."
)

parser.add_argument(
    "--current-date",
    type=str,
    default=CURRENT_DATE.strftime(DATE_FORMAT_STRING),
    help="Latest date to start export from.  ('YYYY-MM-DD')"
)
parser.add_argument(
    "--export-dir",
    type=str,
    default= EXPORT_DIR,
    help="Directory for export files.  Needs to already exist."
)

args = parser.parse_args()
CURRENT_DATE = datetime.datetime.strptime(args.current_date, DATE_FORMAT_STRING)
days = get_date_strings(CURRENT_DATE, DAYS_BACK)
EXPORT_DIR = args.export_dir

for day in days:
    logger.info(f"Consolidating {day} data in {EXPORT_DIR}")
    consolidate_day(EXPORT_DIR, day)

