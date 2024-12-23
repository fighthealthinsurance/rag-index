from .pubmed import load_pubmed
from .arxiv import load_arxiv
from .wikipedia import load_wikipedia
from .loader_utils import load_or_create, run_in_thread
import asyncio

from pyspark.conf import SparkConf
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, pandas_udf, struct, col, split_part, lit
from pyspark.sql.types import *

conf = SparkConf() \
  .set("spark.executor.memory", "40g") \
  .set("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
spark = SparkSession \
  .builder \
  .master("local[5]") \
  .appName("RAGIndex") \
  .config(conf=conf) \
  .getOrCreate()


async def magic(spark: SparkSession):
    main_bloop = await asyncio.gather(
        run_in_thread(load_arxiv, spark),
        run_in_thread(load_wikipedia, spark),
        run_in_thread(load_pubmed, spark)
    )
    return await main_bloop

def create_data_inputs(spark: SparkSession) -> DataFrame:
    dfs = asyncio.run(magic(spark))
    combined = dfs[0].union(dfs[1:])
    return combined

combined = load_or_create(spark, "initial_records", create_data_inputs)
