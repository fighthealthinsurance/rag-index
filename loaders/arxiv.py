import asyncio
import os
import zipfile
import json
from pyspark.sql import DataFrame, SparkSession

from .loader_utils import *

arxiv_file = "arxiv.zip"

async def download_arxiv():
    urls = ["https://www.kaggle.com/api/v1/datasets/download/Cornell-University/arxiv"]
    return await download_file_if_not_existing(arxiv_file, urls)

async def extract_arxiv():
    if not os.path.exists("arxiv-metadata-oai-snapshot.json"):
        await check_call(["unzip", "arxiv.zip"])

def _load_arxiv(spark: SparkSession) -> DataFrame:
    initial = spark.read.format("json").load("arxiv-metadata-oai-snapshot.json")
    relevant_fields = initial.select(
        initial["doi"],
        initial["title"],
        initial["text"],
        initial["id"].alias("arxiv_id"))
    relevant_records = filter_relevant_records_based_on_text(relevant_fields)
    annotated = extract_and_annotate(relevant_records)
    return annotated

async def load_arxiv(spark: SparkSession) -> DataFrame:
    #    await download_arxiv
    #    await extract_arxiv()
    await asyncio.sleep(0)
    return load_or_create(spark, "arxiv", _load_arxiv)
