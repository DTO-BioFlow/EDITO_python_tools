from datetime import datetime
from typing import Any, Optional, Mapping
import csv

import pyarrow.dataset as ds

from ._utils import (
    _resolve_dataset,
    _resolve_columns,
    _build_filter_expression,
)


def read_parquet(
    parquet: ds.Dataset | str,
    columns: list[str] | None = None,
    filters: Mapping[str, Any] | list[tuple[str, Any]] | None = None,
    max_rows: int = 25,
    output_file: Optional[str] = None,
    logs: bool = True,
) -> dict[str, Any]:
    """
    Read and filter parquet data, optionally save to CSV, and display results.

    This function reads only the first max_rows from the parquet file by iterating
    through batches and stopping early. It ensures minimal data is loaded into memory.
    The operation is extremely fast since it doesn't scan the entire file.

    Parameters
    ----------
    parquet : pyarrow.dataset.Dataset or str
        Dataset object, parquet URL/path, or a data-explorer wrapper URL.
    columns : list[str] | None, optional
        Columns to read. If ``None`` or empty, all columns are read.
    filters : mapping | list[tuple[str, Any]] | None, optional
        Row filters applied before reading values.
        Mapping example: ``{"parameter_imisdasid": 4687}``.
        List example: ``[("country", ["NL", "BE"]), ("year", 2024)]``.
    max_rows : int, optional
        Maximum rows to read from the parquet file (default: 25).
        Only these rows are loaded into memory and returned.
    output_file : str | None, optional
        If provided, write the filtered data to this CSV file.
    logs : bool, optional
        If ``True``, print timestamped progress information.

    Returns
    -------
    dict
        Dictionary with keys:
        - 'total_rows': rows read (limited by max_rows)
        - 'displayed_rows': rows shown (should equal total_rows in most cases)
        - 'columns': list of column names
        - 'data': list of dictionaries (rows)
        - 'output_file': path to CSV if written, else None
    """
    dataset_obj = _resolve_dataset(parquet)
    selected_columns = _resolve_columns(dataset_obj, columns)
    filter_expression = _build_filter_expression(dataset_obj, filters)

    if logs:
        print(
            f"{datetime.now()} | start read_parquet | "
            f"columns={len(selected_columns)}"
        )

    # Create scanner with column selection and filters
    # For unfiltered queries on very large files, use smaller batch size to avoid
    # materializing massive batches just to read first N rows
    if filter_expression is None:
        # No filters: use smaller batch size for efficiency
        scanner = dataset_obj.scanner(
            columns=selected_columns,
            batch_size=max_rows * 10  # Read in smaller chunks
        )
    else:
        # With filters: PyArrow will naturally filter efficiently at source
        scanner = dataset_obj.scanner(
            columns=selected_columns,
            filter=filter_expression
        )

    # Read batches until we have enough rows
    display_data = []
    rows_collected = 0

    if logs:
        print(f"{datetime.now()} | reading first {max_rows} rows from parquet")

    for batch in scanner.to_batches():
        if rows_collected >= max_rows:
            break

        batch_list = batch.to_pylist()
        remaining_needed = max_rows - rows_collected
        rows_to_take = min(len(batch_list), remaining_needed)

        display_data.extend(batch_list[:rows_to_take])
        rows_collected += rows_to_take

    total_rows = len(display_data)
    displayed_rows = total_rows

    if logs:
        print(f"{datetime.now()} | read {total_rows} rows total")

    # Write to CSV file if requested
    csv_path = None
    if output_file:
        with open(output_file, "w", newline="", encoding="utf-8") as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=selected_columns)
            csv_writer.writeheader()
            csv_writer.writerows(display_data)
        csv_path = output_file
        if logs:
            print(f"{datetime.now()} | saved {total_rows} rows to {output_file}")

    # Print nicely
    print("\n" + "=" * 80)
    print(f"Parquet Data Summary ({displayed_rows}/{total_rows} rows displayed)")
    print("=" * 80)

    if display_data:
        # Print column headers
        print(" | ".join(f"{col:20s}" for col in selected_columns))
        print("-" * (len(selected_columns) * 22))

        # Print rows
        for row in display_data:
            values = [str(row.get(col, ""))[:20] for col in selected_columns]
            print(" | ".join(f"{val:20s}" for val in values))
    else:
        print("(No rows match the filter criteria)")

    print("=" * 80 + "\n")

    if logs:
        print(f"{datetime.now()} | done read_parquet")

    return {
        "total_rows": total_rows,
        "displayed_rows": displayed_rows,
        "columns": selected_columns,
        "data": display_data,
        "output_file": csv_path,
    }
