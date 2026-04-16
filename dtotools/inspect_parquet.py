from collections import defaultdict
from datetime import datetime
import csv
import json
from typing import Any, Mapping

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow as pa

from ._utils import (
    _resolve_dataset,
    _resolve_columns,
    _build_filter_expression,
    _filter_items,
)


DatasetInput = ds.Dataset | str
FilterInput = Mapping[str, Any] | list[tuple[str, Any]] | None



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


def get_schema(dataset_url: DatasetInput, output_file: str | None = None) -> pa.Schema:
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


