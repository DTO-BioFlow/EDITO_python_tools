from datetime import datetime
from typing import Any, Mapping, Optional
import csv

import duckdb
import pyarrow.compute as pc
import pyarrow.dataset as ds


def _normalize_filters(
    filters: Mapping[str, Any] | list[tuple[str, Any]] | None,
) -> list[tuple[str, Any]]:
    if filters is None:
        return []
    if isinstance(filters, Mapping):
        return list(filters.items())
    if isinstance(filters, list):
        return filters
    raise TypeError("filters must be a mapping, a list of (column, value), or None")


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _resolve_columns_from_names(
    available_columns: list[str], columns: list[str] | None
) -> list[str]:
    if not columns:
        return available_columns

    missing_columns = [name for name in columns if name not in available_columns]
    if missing_columns:
        raise ValueError(f"Unknown columns: {missing_columns}")

    return columns


def _build_sql_filter_expression(
    available_columns: list[str],
    filters: Mapping[str, Any] | list[tuple[str, Any]] | None,
) -> tuple[str | None, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    for column_name, filter_value in _normalize_filters(filters):
        if column_name not in available_columns:
            raise ValueError(f"Unknown filter column: {column_name}")

        quoted_column = _quote_identifier(column_name)
        if isinstance(filter_value, (list, tuple, set, frozenset)):
            values = list(filter_value)
            if not values:
                raise ValueError(
                    f"Filter column '{column_name}' received an empty value list"
                )
            placeholders = ", ".join(["?"] * len(values))
            clauses.append(f"{quoted_column} IN ({placeholders})")
            params.extend(values)
        elif filter_value is None:
            clauses.append(f"{quoted_column} IS NULL")
        else:
            clauses.append(f"{quoted_column} = ?")
            params.append(filter_value)

    if not clauses:
        return None, []

    return " AND ".join(clauses), params


def _build_pyarrow_filter_expression(
    available_columns: list[str],
    filters: Mapping[str, Any] | list[tuple[str, Any]] | None,
) -> pc.Expression | None:
    expression = None

    for column_name, filter_value in _normalize_filters(filters):
        if column_name not in available_columns:
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


def _table_to_rows(table) -> list[dict[str, Any]]:
    return table.to_pylist()


def read_parquet_alt(
    parquet: ds.Dataset | str,
    columns: list[str] | None = None,
    filters: Mapping[str, Any] | list[tuple[str, Any]] | None = None,
    max_rows: int = 25,
    output_file: Optional[str] = None,
    logs: bool = True,
) -> dict[str, Any]:
    """
    Read and filter parquet data using DuckDB, optionally save to CSV, and display results.

    This function keeps the same public signature as the PyArrow-based reader, but
    delegates parquet access to DuckDB for faster metadata-aware filtering and
    limited row retrieval.

    Parameters
    ----------
    parquet : pyarrow.dataset.Dataset or str
        Dataset object or parquet URL/path string.
    columns : list[str] | None, optional
        Columns to read. If ``None`` or empty, all columns are read.
    filters : mapping | list[tuple[str, Any]] | None, optional
        Row filters applied before reading values.
        Mapping example: ``{"parameter_imisdasid": 4687}``.
        List example: ``[("country", ["NL", "BE"]), ("year", 2024)]``.
    max_rows : int, optional
        Maximum rows to read from the parquet source (default: 25).
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
    con = duckdb.connect()
    try:
        if isinstance(parquet, ds.Dataset):
            available_columns = list(parquet.schema.names)
            selected_columns = _resolve_columns_from_names(available_columns, columns)
            filter_expression = _build_pyarrow_filter_expression(
                available_columns, filters
            )
            where_clause = None
            params: list[Any] = []
        elif isinstance(parquet, str):
            metadata_result = con.execute(
                "SELECT * FROM read_parquet(?) LIMIT 0", [parquet]
            )
            available_columns = [desc[0] for desc in metadata_result.description or []]
            selected_columns = _resolve_columns_from_names(available_columns, columns)
            filter_expression = None
            where_clause, params = _build_sql_filter_expression(
                available_columns, filters
            )
        else:
            raise TypeError(
                "parquet must be a pyarrow.dataset.Dataset or a parquet URL/path string"
            )

        if logs:
            print(
                f"{datetime.now()} | start read_parquet_alt | "
                f"columns={len(selected_columns)}"
            )
            print(f"{datetime.now()} | reading first {max_rows} rows from parquet")

        if max_rows <= 0:
            display_data: list[dict[str, Any]] = []
        elif isinstance(parquet, ds.Dataset):
            scanner = parquet.scanner(
                columns=selected_columns,
                filter=filter_expression,
            )
            relation = con.from_arrow(scanner.to_reader())
            table = relation.limit(max_rows).to_arrow_table()
            display_data = _table_to_rows(table)
        else:
            select_clause = ", ".join(_quote_identifier(name) for name in selected_columns)
            query = f"SELECT {select_clause} FROM read_parquet(?)"
            query_params: list[Any] = [parquet]
            if where_clause is not None:
                query += f" WHERE {where_clause}"
                query_params.extend(params)
            query += " LIMIT ?"
            query_params.append(max_rows)

            table = con.execute(query, query_params).to_arrow_table()
            display_data = _table_to_rows(table)

        total_rows = len(display_data)
        displayed_rows = total_rows

        if logs:
            print(f"{datetime.now()} | read {total_rows} rows total")

        csv_path = None
        if output_file:
            with open(output_file, "w", newline="", encoding="utf-8") as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=selected_columns)
                csv_writer.writeheader()
                csv_writer.writerows(display_data)
            csv_path = output_file
            if logs:
                print(f"{datetime.now()} | saved {total_rows} rows to {output_file}")

        print("\n" + "=" * 80)
        print(f"Parquet Data Summary ({displayed_rows}/{total_rows} rows displayed)")
        print("=" * 80)

        if display_data:
            print(" | ".join(f"{col:20s}" for col in selected_columns))
            print("-" * (len(selected_columns) * 22))
            for row in display_data:
                values = [str(row.get(col, ""))[:20] for col in selected_columns]
                print(" | ".join(f"{val:20s}" for val in values))
        else:
            print("(No rows match the filter criteria)")

        print("=" * 80 + "\n")

        if logs:
            print(f"{datetime.now()} | done read_parquet_alt")

        return {
            "total_rows": total_rows,
            "displayed_rows": displayed_rows,
            "columns": selected_columns,
            "data": display_data,
            "output_file": csv_path,
        }
    finally:
        con.close()

