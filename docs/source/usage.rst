Usage
==================

Search a dataset on title in a specific collection
--------------------------------------------------

.. code-block:: python

    from dtotools.search import search_on_title
    results = search_on_title(title="koster", collection="emodnet-biology")
    print(results)

This will return

.. code-block:: python

    [<Item id=bdbeb221-7656-52e5-9ade-4b3304db82cd>]

This Item is `a pystac item`_ which can be further explored using `PySTAC library`_ .

.. _a pystac item: https://pystac.readthedocs.io/en/stable/api/item.html
.. _PySTAC library: https://pystac.readthedocs.io/en/stable/index.html


Search a dataset on title in all collections
--------------------------------------------

.. code-block:: python

    from dtotools.search import search_on_title
    results = search_on_title(title="koster")



Inspect a parquet file
----------------------

.. code-block:: python

    from dtotools.inspect_parquet import inspect_parquet
    inspect_parquet("https://s3.waw3-1.cloudferro.com/emodnet/emodnet_biology/12639/marine_biodiversity_observations_2026-02-26.parquet)

    inspect_parquet(
        dataset=DATASET_URL,
        columns=["parameter"],
        filters=[("parameter_imisdasid", [4687])],
        output_file="output/inspect_parquet_0.csv"
        )

This will result in

.. code-block:: python
    column_name,column_type,unique_values
    parameter,string,"[{""value"": ""Detritus (#/l)"", ""count"": 27594}, {""value"": ""Diameter_sample_collector_aperture (cm)"", ""count"": 25644}, {""value"": ""Fibres (#/l)"", ""count"": 27594}, {""value"": ""LifeStage"", ""count"": 27552}, {""value"": ""Mesh_size (um)"", ""count"": 25644}, {""value"": ""Samp_vol (l)"", ""count"": 27540}, {""value"": ""sampling_instrument_name"", ""count"": 26007}, {""value"": ""sampling_platform_name"", ""count"": 27927}, {""value"": ""SubSamplingCoefficient (Dmnless)"", ""count"": 27429}, {""value"": ""unidentified_biota (#/l)"", ""count"": 27594}, {""value"": ""WaterAbund (#/ml)"", ""count"": 27582}]"
