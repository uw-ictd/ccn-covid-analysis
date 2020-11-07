#! /usr/bin/env python3

import argparse

import fastparquet


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read and display metadata from parquet")
    parser.add_argument("filename", nargs=1, help="The name of the file to read")

    args = parser.parse_args()

    filename = args.filename[0]
    print(filename)

    file_parsed = fastparquet.ParquetFile(filename, verify=True)

    print("Info for " + filename)
    print(file_parsed.info)
    print(file_parsed.columns)
    print(file_parsed.dtypes)
    print("Self Made:", file_parsed.selfmade)
