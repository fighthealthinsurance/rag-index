import re
import os
from lxml import etree
import asyncio
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import element_at, regexp, lit, regexp_extract_all

from .loader_utils import *


class WikipediaDataSource(CompressedRagDataSource):
    wiki_date = f"20241201"
    filename = f"enwiki-{wiki_date}-pages-articles-multistream.xml.bz2"
    extracted_filename = f"enwiki-{wiki_date}-pages-articles-multistream.xml"
    input_format = "xml"
    input_options = {"rowTag": "page"}
    name = "wikipedia"

    wikimedia_mirrors = [
        f"https://dumps.wikimedia.org/enwiki/{wiki_date}/",
        f"https://dumps.wikimedia.your.org/enwiki/{wiki_date}/"]
    urls = list(
        map(lambda base: f"{base}{wiki_multistream_filename}",
            wikimedia_mirrors))

    async def _filter(df: DataFrame) -> DataFrame:
        internal_filtered = df.filter(df["redirect"].isNull())
        return await super()._filtered(internal_filtered)
    
    async def _select(df: DataFrame) -> DataFrame:
        relevant_fields = non_redirected.select(
            non_redirected["title"],
            non_redirected["revision"]["text"]["_VALUE"].alias("text")
        )
