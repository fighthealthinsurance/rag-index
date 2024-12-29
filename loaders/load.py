from .pubmed import PubMedDataSource
from .medline import MedlineDataSource
from .arxiv import ArxivDataSource
from .wikipedia import WikipediaDataSource
from .loader_utils import load_or_create, executor
from .spark_session import spark

import asyncio
from pyspark.sql import DataFrame, SparkSession


async def magic(spark: SparkSession) -> list[DataFrame]:
    data_sources = [
        ArxivDataSource(),
        PubMedDataSource(),
        MedlineDataSource(),
        WikipediaDataSource(),
    ]
    results = map(lambda x: x.load(spark), data_sources)
    main_bloop: list[DataFrame] = await asyncio.gather(*results)
    return main_bloop


async def create_data_inputs(spark: SparkSession) -> DataFrame:
    dfs = await magic(spark)
    if len(dfs) > 1:
        combined = dfs[0].union(*dfs[1:])
        return combined
    else:
        return dfs[0]


combined = asyncio.run(load_or_create(spark, "initial_records", create_data_inputs))
