import re
import os
from lxml import etree
import mwxml
import asyncio
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import element_at, regexp, lit, regexp_extract_all

from .loader_utils import *

wiki_date = f"20241201"
wiki_multistream_filename = f"enwiki-{wiki_date}-pages-articles-multistream.xml.bz2"

async def download_wikipedia() -> None:
    wikimedia_mirrors = [
        f"https://dumps.wikimedia.org/enwiki/{wiki_date}/",
        f"https://dumps.wikimedia.your.org/enwiki/{wiki_date}/"]
    urls = list(
        map(lambda base: f"{base}{wiki_multistream_filename}",
            wikimedia_mirrors))
    await download_file_if_not_existing(wiki_multistream_filename, urls)

def wikipedia_df(spark) -> DataFrame:
    pubmed_df = spark \
        .read \
        .format("xml") \
        .options(rowTag="page") \
        .load(wiki_multistream_filename)
    return pubmed_df

async def load_wikipedia(spark) -> DataFrame:
    await asyncio.sleep(0)
    df = wikipedia_df(spark)
    non_redirected = df.filter(dfs[0]["redirect"].isNull())
    relevant_fields = non_redirected.select(
        non_redirected["title"],
        non_redirected["revision"]["text"]["_VALUE"].alias("text")
    )
    relevant_records = filter_relevant_records_based_on_text(relevant_fields)
    annotated = extract_and_annotate(relevant_records)
    return annotated
