"""
This is the docstring for the package.
"""
import json


def extract_version_from_file(file_path):
    try:
        # Open and read the JSON file
        with open(file_path, 'r') as file:
            data = json.load(file)

        # Extract and return the version
        return data.get('version', 'Version not found')

    except FileNotFoundError:
        return f"Error: The file '{file_path}' was not found."

    except json.JSONDecodeError:
        return "Error: The file could not be decoded as JSON."

    except Exception as e:
        return f"An unexpected error occurred: {e}"


codemeta = "../codemeta.json"
version = extract_version_from_file(codemeta)

from .search import search_on_title
from .inspect_parquet import get_schema, inspect_parquet
