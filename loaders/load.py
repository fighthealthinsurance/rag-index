from .pubmed import load_pubmed
from .arxiv import load_arxiv
from .wikipedia import load_wikipedia
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


async def magic():
    awaitables = [
        load_wikipedia(spark),
        load_arxiv(spark),
        load_pubmed(spark)]
    main_bloop = asyncio.gather(*awaitables)
    return await main_bloop

dfs = asyncio.run(magic())
combined = dfs[0].union(dfs[1:])
