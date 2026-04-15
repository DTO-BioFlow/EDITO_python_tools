from dtotools.inspect_parquet import inspect_parquet

DATASET_URL = ("https://s3.waw3-1.cloudferro.com/emodnet/emodnet_biology"
               "/12639/marine_biodiversity_observations_2026-02-26.parquet")

inspect_parquet(
    dataset=DATASET_URL,
    columns=["parameter"],
    filters=[("parameter_imisdasid", [4687])],
    output_file="output/inspect_parquet_0.csv"
)

