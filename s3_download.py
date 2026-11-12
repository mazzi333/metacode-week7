import argparse
import csv
from pathlib import Path

import boto3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("bucket")
    args = parser.parse_args()

    bucket = args.bucket
    prefix = "bronze/"
    key = f"{prefix}netflix_titles.csv"
    output = Path(__file__).resolve().parent / "data" / "netflix_titles.csv"
    s3 = boto3.client("s3")

    print(f"[1] List s3://{bucket}/{prefix}")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            print(f"    {obj['Key']}  {obj['Size']:,} bytes")

    print(f"[2] Download s3://{bucket}/{key} -> data/netflix_titles.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(output))
    print(f"    Downloaded {output.stat().st_size:,} bytes")

    with output.open(encoding="utf-8-sig", newline="") as file:
        records = csv.reader(file)
        next(records, None)
        count = sum(1 for row in records if row)

    print(f"[3] CSV records (excluding header): {count:,}")


if __name__ == "__main__":
    main()