import csv
import io
import os
import queue
import sys
import zipfile

from datetime import datetime
from pathlib import Path

class FxParser:
    def __init__(self, conn, user, csv_file, data_dir):
        self.conn = conn
        self.user = user
        self.csv_file = csv_file
        self.data_dir = data_dir
        self.pair_codes = []
        self.pair_directories = []# Perparpe the workspace for the parser
        self.ReadCodesCsv()
        self.CheckDataFolders()
        self.Pairdata2Pgsql()

    def FormatDatetime(self, datetime_str):
        """ Formats a FX 1 Minute Data CSV timestamp (EST) into a datatime string for use with the pgsql database"""
        # Parse the datetime string
        dt = datetime.strptime(datetime_str, '%Y%m%d %H%M%S')
        # Format the datetime as 'YYYY-MM-DD HH:MM:SS'
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    def ReadCodesCsv(self):
        """ Reads the possible pair codes .csv file """
        with open(self.csv_file, mode='r') as fx_pairs:
            csv_reader = csv.reader(fx_pairs)
            next(csv_reader)
            self.pair_codes = [row[1] for row in csv_reader]

    def CheckDataFolders(self):
        """ Checks that data directories are availble from the list of pair """
        _pair_codes = self.pair_codes
        self.pair_codes = []
        for code in _pair_codes:
            pair_data_path  = Path(self.data_dir, code)
            if os.path.exists(pair_data_path):
                self.pair_codes.append(code)
                self.pair_directories.append(pair_data_path)

    def ListAndExtractCsvInMemory(self, zip_path):
        """List the contents of a ZIP file and extract a CSV file in memory."""
        row_queue = queue.Queue(maxsize=0)
        # Read the zip file into memory
        with zip_path.open('rb') as file:
            with zipfile.ZipFile(io.BytesIO(file.read())) as zip_ref:
                # List the contents of the ZIP file
                zip_files = zip_ref.namelist()
                # Extract CSV file in memory
                csv_files = [f for f in zip_files if f.endswith('.csv')]
                # Put the returned rows into a queue object to be processes
                for f in csv_files:
                    with zip_ref.open(f) as csv_file:
                        content = csv_file.read().decode()
                        reader = csv.reader(content.splitlines(), delimiter=';')
                        for row in reader:
                            sanitized_data = []
                            sanitized_data.append(self.FormatDatetime(row[0]))
                            sanitized_data.append((float(row[1]) + float(row[2]) + float(row[3]) + float(row[4])) / 4)
                            row_queue.put(sanitized_data)
        return row_queue

    def ListPairDataFiles(self, directory):
        """ List the pairs data files that could possible be used """
        return [f for f in Path(directory).iterdir() if f.is_file() and f.suffix == '.zip']

    def CreatePairTable(self,  pair_name):
        """ Create a table for the pair data """
        # Create a new cursor
        cur = self.conn.cursor()

        # Construct the SQL query
        sql = f"""
            -- DROP TABLE IF EXISTS pairs.{pair_name};

            CREATE TABLE IF NOT EXISTS pairs.{pair_name}
            (
                ts timestamp with time zone NOT NULL,
                value numeric NOT NULL,
                CONSTRAINT {pair_name}_pkey PRIMARY KEY (ts)
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS pairs.{pair_name}
                OWNER to {self.user};
        """

        try:
            # Execute the SQL query
            cur.execute(sql)
            # Commit the transaction
            self.conn.commit()
        except Exception as e:
            # If an error occurs, rollback the transaction
            self.conn.rollback()
            print(f"An error occurred: {e}")
        finally:
            # Close the cursor
            cur.close()

    def InsertPairTick(self, table_name, data):
        """ Insert a pairs tick data to the database  """
        # Create a new cursor
        cur = self.conn.cursor()

        # Construct the SQL query with placeholders for data
        sql = f"""
            INSERT INTO pairs.{table_name} (ts, value)
            VALUES (%s, %s)
            ON CONFLICT (ts) DO NOTHING;
        """

        try:
            # Execute the SQL query with data
            cur.execute(sql, (data[0], data[1]))
            # Commit the transaction
            self.conn.commit()
        except Exception as e:
            # If an error occurs, rollback the transaction
            self.conn.rollback()
            print(f"An error occurred: {e}")
        finally:
            # Close the cursor
            cur.close()

    def Pairdata2Pgsql(self):
        """ Get and write the pair data to the postgresql database  """
        for idx, pair_dir in enumerate(self.pair_directories):
            self.CreatePairTable(self.pair_codes[idx])
            for file in self.ListPairDataFiles(pair_dir):
                rows = self.ListAndExtractCsvInMemory(file)
                while not rows.empty():
                    row = rows.get()
                    self.InsertPairTick(self.pair_codes[idx], row)
                    rows.task_done()


if __name__ == "__main__":
    print("This class should not be executed as __main__.")
    sys.exit(1)


