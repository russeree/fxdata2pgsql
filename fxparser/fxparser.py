import csv
import os
import sys

class FxParser:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.pair_codes = []
        self.read_codes_csv()
        print(self.pair_codes)

    def read_codes_csv(self):
        with open(self.csv_file, mode='r') as fx_pairs:
            csv_reader = csv.reader(fx_pairs)
            next(csv_reader)
            self.pair_codes = [row[1] for row in csv_reader]

if __name__ == "__main__":
    print("This class should not be executed as __main__.")
    sys.exit(1)

