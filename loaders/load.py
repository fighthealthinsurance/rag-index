import asyncio

from pyspark.sql import DataFrame, SparkSession

from .arxiv import ArxivDataSource
from .loader_utils import executor, load_or_create
from .medline import MedlineDataSource
from .pubmed import PubMedDataSource
from .spark_session import spark
from .wikipedia import WikipediaDataSource
from .legacy_cdc import LegacyCDC
from .loader_utils import mini_pipeline


async def magic(spark: SparkSession) -> list[DataFrame]:
    data_sources = [
        ArxivDataSource(),
        PubMedDataSource(),
#        MedlineDataSource(),
        WikipediaDataSource(),
    ]
    if mini_pipeline:
        data_sources = [
            PubMedDataSource(),
#            MedlineDataSource(),
#            LegacyCDC(),
        ]
    results = map(lambda x: x.load(spark), data_sources)
    main_bloop: list[DataFrame] = await asyncio.gather(*results)
    print(f"Got dataframes {main_bloop}")
    return main_bloop


async def create_data_inputs(spark: SparkSession) -> DataFrame:
    dfs = await magic(spark)
    if len(dfs) > 1:
        combined = dfs[0]
        for df in dfs[1:]:
            combined = combined.unionByName(df, allowMissingColumns=True)
        return combined
    else:
        return dfs[0]


combined = asyncio.run(
    load_or_create(spark, "initial_records", create_data_inputs), debug=True
)
