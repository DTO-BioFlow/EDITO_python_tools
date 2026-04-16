Usage
==================

Search a dataset on title in a specific collection
--------------------------------------------------

.. code-block:: python

    from dtotools.search import search_on_title

    results = search_on_title(title="koster", collection="emodnet-biology")
    print(results)

This will return:

.. code-block:: python

    [<Item id=bdbeb221-7656-52e5-9ade-4b3304db82cd>]

This Item is a pystac item which can be further explored using PySTAC library.

.. _a pystac item: https://pystac.readthedocs.io/en/stable/api/item.html
.. _PySTAC library: https://pystac.readthedocs.io/en/stable/index.html


Search a dataset on title in all collections
--------------------------------------------

.. code-block:: python

    from dtotools.search import search_on_title

    results = search_on_title(title="koster")


Inspect a parquet file
----------------------

Read a parquet file without filtering:

.. code-block:: python

    from dtotools.inspect_parquet import inspect_parquet

    DATASET_URL = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_biology/12639/marine_biodiversity_observations_2026-02-26.parquet"

    inspect_parquet(DATASET_URL)


Read a parquet file with filtering:

.. code-block:: python

    from dtotools.inspect_parquet import inspect_parquet

    DATASET_URL = "https://s3.waw3-1.cloudferro.com/emodnet/emodnet_biology/12639/marine_biodiversity_observations_2026-02-26.parquet"

    result = inspect_parquet(
        dataset=DATASET_URL,
        columns=["parameter"],
        filters=[("parameter_imisdasid", [4687])],
        output_file="output/inspect_parquet_0.csv"
    )