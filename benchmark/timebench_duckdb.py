import timeit

from dtotools.read_parquet_alt import read_parquet_alt

DATASET_URL = ("https://s3.waw3-1.cloudferro.com/emodnet/emodnet_biology"
               "/12639/marine_biodiversity_observations_2026-02-26.parquet")

def bench0():
    read_parquet_alt(parquet=DATASET_URL, max_rows=10, logs=False)

def bench1():
    read_parquet_alt(
        parquet=DATASET_URL,
        filters={"datasetid": 4687},
        max_rows=50,
        logs=False,
    )

def bench2():
    read_parquet_alt(
        parquet=DATASET_URL,
        columns=["datasetid"],
        filters={"datasetid": 4687},
        max_rows=50,
        logs=False,
    )


if __name__ == "__main__":
    runs = 10
    tests = {
        "test0": bench0,
        "test1": bench1,
        "test2": bench2,
    }

    print(f"Running each test {runs} times...\n")

    results = []

    for name, func in tests.items():
        total_seconds = timeit.Timer(func).timeit(number=runs)
        avg_seconds = total_seconds / runs

        results.append(
            f"{name:>6} | total: {total_seconds:8.3f} s | "
            f"avg: {avg_seconds * 1000:8.2f} ms"
        )

    # single consolidated output
    print("\nResults:\n")
    print("\n".join(results))
