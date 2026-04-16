from importlib.metadata import PackageNotFoundError, version

try:
	__version__ = version("dtotools")
except PackageNotFoundError:
	__version__ = "0.0.0"

from .search import search_on_title
from .inspect_parquet import get_schema, inspect_parquet
from .read_parquet import read_parquet
from .read_parquet_alt import read_parquet_alt
