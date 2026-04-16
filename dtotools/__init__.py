from importlib.metadata import version
__version__ = version("your-package-name")

from .search import search_on_title
from .inspect_parquet import get_schema, inspect_parquet
