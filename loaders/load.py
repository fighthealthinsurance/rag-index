from .pubmed import PubMedDataSource
from .medline import MedlineDataSource
from .arxiv import ArxivDataSource
from .wikipedia import WikipediaDataSource
from .loader_utils import load_or_create, executor
import asyncio

from pyspark.conf import SparkConf
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, pandas_udf, struct, col, split_part, lit
from pyspark.sql.types import *

conf = SparkConf() \
  .set("spark.executor.memory", "20g") \
  .set("spark.driver.memory", "40g") \
  .set("spark.executor.cores", "1") \
  .set("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
spark = SparkSession \
  .builder \
  .master("local[15]") \
  .appName("RAGIndex") \
  .config(conf=conf) \
  .getOrCreate()


async def magic(spark: SparkSession) -> list[DataFrame]:
    data_sources = [
        ArxivDataSource(),
#        PubMedDataSource(),
        MedlineDataSource(),
        WikipediaDataSource()
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

combined = asyncio.run(
#    load_or_create(
#        spark,
#        "initial_records",
        create_data_inputs(spark))
    #))
