import os
from pyspark.sql import DataFrame, SparkSession

from .loader_utils import *

async def download_pubmed():
    await download_recursive([
        "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/",
        "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"
    ])


def _load_pubmed(spark: SparkSession) -> DataFrame:
    pubmed_df = spark \
        .read \
        .format("xml") \
        .options(rowTag="PubmedArticle") \
        .load("./recursive/*.xml.gz")
    return pubmed_df

async def load_pubmed(spark: SparkSession) -> DataFrame:
    await asyncio.sleep(0)
    return load_or_create(spark, "pubmed", _load_pubmed)
