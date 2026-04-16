from pystac_client import Client
from typing import Optional, List
import pystac
from datetime import datetime as dt
from functools import lru_cache

_COLLECTION_URL = "https://api.dive.edito.eu/data/collections"


@lru_cache(maxsize=1)
def _get_catalog() -> Client:
    return Client.open(_COLLECTION_URL)


def get_collection_url() -> str:
    """
    Return the base URL of the STAC collections endpoint.

    This function exposes the configured STAC collections endpoint used by the
    underlying pystac client. It can be useful for logging, debugging, or
    validation purposes.

    Returns
    -------
    str
        The base URL of the STAC collections endpoint.
    """
    return _COLLECTION_URL


def search_on_title(
    title: str,
    collection: Optional[str] = None,
    verbose: int = 0
) -> List[pystac.Item]:
    """
    Search STAC items whose title contains a given substring.

    This function performs a client-side scan of one or more STAC collections
    and returns all items whose ``title`` property contains the provided search
    string (case-insensitive). Since the target STAC API does not support the
    ``/search`` endpoint, server-side filtering is unavailable and a full
    collection scan is required.

    Parameters
    ----------
    title : str
        Substring to search for within the item ``title`` field. The match is
        performed in a case-insensitive manner.
    collection : str, optional
        Identifier of a specific collection to search. If not provided, all
        available collections in the catalog are searched.
    verbose : int, optional
        Verbosity level controlling console output:

        - ``0``: silent mode (default)
        - ``>0``: print progress and status messages

    Returns
    -------
    List[pystac.Item]
        A list of STAC items whose ``title`` field contains the specified
        substring.

    Raises
    ------
    ValueError
        If the specified collection identifier does not exist in the catalog.

    Notes
    -----
    This implementation performs a brute-force scan over collections and items
    due to the lack of support for the STAC ``ITEM_SEARCH`` conformance class
    by the target API. For large catalogs, this operation may be slow and
    network-bound.

    Examples
    --------
    Search all collections for items containing ``"koster"`` in their title:

    >>> items = search_on_title("koster", verbose=1)

    Search only within a specific collection:

    >>> items = search_on_title("koster", collection="emodnet-biology")
    """
    results = []
    item_counter = 0
    catalog = _get_catalog()

    if collection:
        if verbose > 0:
            print(f"{dt.now()} | loading collection: {collection}...")
        col_client = catalog.get_collection(collection)
        if col_client is None:
            raise ValueError(f"Collection '{collection}' not found.")
        collections = [col_client]
    else:
        if verbose > 0:
            print(f"{dt.now()} | loading all collections...")
        collections = list(catalog.get_all_collections())

    total_collections = len(collections)

    for col_idx, col_client in enumerate(collections, start=1):
        print("-"*50)
        print(
            f"{dt.now()} | Collection {col_idx}/"
            f"{total_collections}: {col_client.title}"
        )

        try:
            items_list = list(col_client.get_items())
        except Exception as e:
            print(
                f"{dt.now()} | Warning: Could not fetch items from collection "
                f"{col_client.id}: {e}"
            )
            continue

        if verbose > 0:
            print(f"{dt.now()} | scanning {len(items_list)} items...")
        item_counter += len(items_list)

        for item in items_list:
            if title.lower() in item.properties.get("title", "").lower():
                if verbose > 0:
                    print(f"{dt.now()} | >>> found matching item: {item.properties.get('title')}")
                results.append(item)

    if verbose > 0:
        print("###########")
        print("# SUMMARY #")
        print("###########")
        print(f"searched a total of {item_counter} items")
        print(f"found {len(results)} results")
        for item in results:
            print(f"- {item.properties.get('title')}")

    return results


# now working:
# Server does not conform to ITEM_SEARCH, There is no fallback option
# available for search

# def search_on_title_fast(
#     title: str,
#     collection: Optional[str] = None,
#     verbose: int = 0
# ) -> List[pystac.Item]:
#
#     if verbose:
#         print(f"{dt.now()} | Server-side search for title='{title}'")
#
#     search = _catalog.search(
#         collections=[collection] if collection else None,
#         query={"title": {"ilike": f"%{title}%"}}
#     )
#
#     results = list(search.get_items())
#
#     if verbose:
#         print(f"{dt.now()} | Found {len(results)} matching items")
#
#     return results

