"""Example of using read_parquet with filtering and column selection."""

from dtotools import read_parquet
import os

DATASET_URL = ("https://s3.waw3-1.cloudferro.com/emodnet/emodnet_biology"
               "/12639/marine_biodiversity_observations_2026-02-26.parquet")

result = read_parquet(parquet= DATASET_URL, max_rows=10)

print(f"Total rows: {result['total_rows']}")
print(f"Columns: {result['columns']}")

