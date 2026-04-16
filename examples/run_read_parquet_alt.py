from dtotools.read_parquet import read_parquet
DATASET_URL = ("https://s3.waw3-1.cloudferro.com/emodnet/emodnet_biology"
               "/12639/marine_biodiversity_observations_2026-02-26.parquet")


result = read_parquet(parquet= DATASET_URL, max_rows=10)

# result = read_parquet(
#     parquet=DATASET_URL,
#     filters={"datasetid": 4687},
#     max_rows=50
# )
#
#
# result = read_parquet(
#     parquet=DATASET_URL,
#     columns=["datasetid"],
#     filters={"datasetid": 4687},
#     max_rows=50
# )