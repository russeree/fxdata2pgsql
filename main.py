import os
import psycopg2
import sys

from dotenv import load_dotenv
from fxparser.fxparser import FxParser
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

PG_HOST = os.getenv('PG_HOST')
PG_PORT = int(os.getenv('PG_PORT', 5432))
PG_USER = os.getenv('PG_USER')
PG_PASS = os.getenv('PG_PASS')
PG_DB   = os.getenv('PG_DB')


def CheckPathExists(path):
    if not path.exists():
        raise FileNotFoundError(f"The path '{path}' does not exist.")

def CreateSchema():
    # Establish connection
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASS,
        dbname=PG_DB
    )

    # Create a new cursor
    cur = conn.cursor()

    # SQL statement to create schema if it doesn't exist
    create_schema_sql = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = 'pairs'
        )
        THEN
            EXECUTE 'CREATE SCHEMA pairs';
        END IF;
    END $$;
    """

    # Execute the SQL statement
    cur.execute(create_schema_sql)

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()

if __name__ == "__main__":
    #generate the paths needed to parse FX_1MIN data into the PGSQL database
    repo_path = Path(os.path.expanduser(os.getenv('FX_1MIN_REPO_PATH'))) / os.getenv("FX_1MIN_PAIR_CODES_FILENAME")

    try:
        CheckPathExists(repo_path)
    except FileNotFoundError as fnfe:
        print(f"FX pair codes file {repo_path} does not exist.")
        sys.exit(1)

    fxp = FxParser(repo_path)

    print(repo_path)
    CreateSchema()
