import os
from dtotools.inspect_parquet import get_schema

DATASET_URL = ("https://s3.waw3-1.cloudferro.com/emodnet/emodnet_biology/12639/marine_biodiversity_observations_2026-02-26.parquet"
)

OUT_DIR = "output"
if not os.path.exists(OUT_DIR):
	os.makedirs(OUT_DIR)

schema_output = os.path.join(OUT_DIR, "schema.txt")

get_schema(DATASET_URL, output_file=str(schema_output))
print(f"Schema written to: {schema_output}")





