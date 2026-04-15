from urllib.parse import parse_qs, urlparse
from urllib.request import url2pathname
from collections import defaultdict
from datetime import datetime
import csv
import json
from typing import Any, Mapping

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs


DatasetInput = ds.Dataset | str
FilterInput = Mapping[str, Any] | list[tuple[str, Any]] | None




def _get_dataset(dataset_url: str) -> ds.Dataset:
    """
    Build a PyArrow dataset from a direct URL or local path.

    Parameters
    ----------
    dataset_url : str
        A direct parquet URL, a data-explorer wrapper URL, or a local path.

    Returns
    -------
    pyarrow.dataset.Dataset
        The dataset object used by the public inspection functions.
    """
    parsed_url = urlparse(dataset_url)

    if parsed_url.scheme in {"http", "https"} and parsed_url.hostname:
        path_parts = parsed_url.path.strip("/").split("/", 1)
        if len(path_parts) != 2:
            raise ValueError(
                "Expected S3 URL with bucket and key, got: "
                f"{dataset_url}"
            )

        bucket_name, key = path_parts
        s3 = fs.S3FileSystem(endpoint_override=parsed_url.hostname, anonymous=True)
        dataset_path = f"{bucket_name}/{key}"
        return ds.dataset(dataset_path, filesystem=s3, format="parquet")

    if parsed_url.scheme == "file":
        local_path = url2pathname(parsed_url.path)
        if parsed_url.netloc:
            local_path = f"{parsed_url.netloc}{local_path}"
        return ds.dataset(local_path, format="parquet")
    return ds.dataset(dataset_url, format="parquet")


def _resolve_dataset(dataset_input: DatasetInput) -> ds.Dataset:
    if isinstance(dataset_input, ds.Dataset):
        return dataset_input
    if isinstance(dataset_input, str):
        return _get_dataset(dataset_input)
    raise TypeError(
        "dataset_input must be a pyarrow.dataset.Dataset or a parquet URL/path string"
    )


def _resolve_columns(dataset: ds.Dataset, columns: list[str] | None) -> list[str]:
    if not columns:
        return [field.name for field in dataset.schema]

    missing_columns = [name for name in columns if name not in dataset.schema.names]
    if missing_columns:
        raise ValueError(f"Unknown columns: {missing_columns}")

    return columns


def _filter_items(filters: FilterInput) -> list[tuple[str, Any]]:
    if filters is None:
        return []
    if isinstance(filters, Mapping):
        return list(filters.items())
    if isinstance(filters, list):
        return filters
    raise TypeError("filters must be a mapping, a list of (column, value), or None")


def _build_filter_expression(
    dataset: ds.Dataset,
    filters: FilterInput,
) -> ds.Expression | None:
    expression = None
    for column_name, filter_value in _filter_items(filters):
        if column_name not in dataset.schema.names:
            raise ValueError(f"Unknown filter column: {column_name}")

        if isinstance(filter_value, (list, tuple, set, frozenset)):
            values = list(filter_value)
            if not values:
                raise ValueError(
                    f"Filter column '{column_name}' received an empty value list"
                )
            condition = pc.field(column_name).isin(values)
        elif filter_value is None:
            condition = pc.field(column_name).is_null()
        else:
            condition = pc.field(column_name) == filter_value

        expression = condition if expression is None else expression & condition

    return expression


def _row_from_counts(
    dataset: ds.Dataset,
    column_name: str,
    value_counts: defaultdict[Any, int],
) -> list[str]:
    sorted_values = sorted(value_counts.items(), key=lambda item: _value_sort_key(item[0]))
    values_payload = [
        {"value": value, "count": count}
        for value, count in sorted_values
    ]

    return [
        column_name,
        str(dataset.schema.field(column_name).type),
        json.dumps(values_payload, ensure_ascii=True),
    ]


def get_schema(dataset_url: DatasetInput, output_file: str | None = None) -> ds.Schema:
    """
    Print and return the parquet schema.

    Parameters
    ----------
    dataset_url : pyarrow.dataset.Dataset or str
        Dataset object, parquet URL/path, or a data-explorer wrapper URL.
    output_file : str | None, optional
        If provided, a CSV file is written with columns ``name`` and ``dtype``.
        The default is ``None``.

    Returns
    -------
    pyarrow.lib.Schema
        The parquet schema.

    Examples
    --------
    >>> schema = get_schema("file:///tmp/example.parquet")
    """
    dataset = _resolve_dataset(dataset_url)
    schema = dataset.schema

    for field in schema:
        print(f"{field.name}: {field.type}")

    if output_file is not None:
        with open(output_file, "w", newline="", encoding="utf-8") as file_handle:
            writer = csv.writer(file_handle)
            writer.writerow(["name", "dtype"])
            for field in schema:
                writer.writerow([field.name, str(field.type)])

    return schema


def _value_sort_key(value: Any) -> tuple[int, str]:
    if value is None:
        return (0, "")
    return (1, str(value).casefold())


def inspect_parquet(
    dataset: DatasetInput,
    output_file: str,
    columns: list[str] | None = None,
    filters: FilterInput = None,
    logs: bool = True,
) -> str:
    """
    Inspect parquet data and write unique values/counts per column to CSV.

    Parameters
    ----------
    dataset : pyarrow.dataset.Dataset or str
        Dataset object, parquet URL/path, or data-explorer wrapper URL.
    output_file : str
        Destination CSV file.
    columns : list[str] | None, optional
        Columns to inspect. If ``None`` or empty, all columns are inspected.
    filters : mapping | list[tuple[str, Any]] | None, optional
        Row filters applied before counting values.
        Mapping example: ``{"parameter_imisdasid": 4687}``.
        List example: ``[("country", ["NL", "BE"]), ("year", 2024)]``.
    logs : bool, optional
        If ``True``, print timestamped progress information.

    Returns
    -------
    str
        The path written to ``output_file``.
    """
    dataset_obj = _resolve_dataset(dataset)
    selected_columns = _resolve_columns(dataset_obj, columns)
    filter_expression = _build_filter_expression(dataset_obj, filters)

    if logs:
        print(
            f"{datetime.now()} | start inspect_parquet | "
            f"columns={len(selected_columns)}"
        )

    counts_by_column: dict[str, defaultdict[Any, int]] = {
        column_name: defaultdict(int)
        for column_name in selected_columns
    }

    scanner = dataset_obj.scanner(columns=selected_columns, filter=filter_expression)
    for batch_index, batch in enumerate(scanner.to_batches(), start=1):
        if logs:
            print(f"{datetime.now()} | inspect_parquet | batch {batch_index}")
        for column_name in selected_columns:
            counts_struct = pc.value_counts(batch[column_name]).to_pylist()
            for item in counts_struct:
                counts_by_column[column_name][item["values"]] += int(item["counts"])

    with open(output_file, "w", newline="", encoding="utf-8") as file_handle:
        writer = csv.writer(file_handle)
        writer.writerow(["column_name", "column_type", "unique_values"])
        for column_name in selected_columns:
            writer.writerow(
                _row_from_counts(dataset_obj, column_name, counts_by_column[column_name])
            )

    if logs:
        print(f"{datetime.now()} | done inspect_parquet")

    return output_file


