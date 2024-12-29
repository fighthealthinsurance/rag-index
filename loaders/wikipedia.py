import re
import os
from lxml import etree
import asyncio
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import element_at, regexp, lit, regexp_extract_all

from .loader_utils import *
from .rag_datasource import *


class WikipediaDataSource(CompressedRagDataSource):
    wiki_date = f"20241201"
    extracted_filename = f"enwiki-{wiki_date}-pages-articles-multistream.xml"
    filename = f"{extracted_filename}.bz2"
    input_format = "com.databricks.spark.xml"
    input_options = {"rowTag": "page", "rootTag": "mediawiki"}
    name = "wikipedia"
    decompress_needed = False  # bzip2 is splittable.

    wikimedia_mirrors = [
        f"https://dumps.wikimedia.org/enwiki/{wiki_date}/",
        f"https://dumps.wikimedia.your.org/enwiki/{wiki_date}/",
    ]

    # Use `classmethod` to resolve `filename` properly.
    @property
    def urls(self):
        return list(map(lambda base: f"{base}{self.filename}", self.wikimedia_mirrors))

    async def _filter(self, df: DataFrame) -> DataFrame:
        """Select records which are not redirects & meet the super reqs."""
        await asyncio.sleep(0)
        internal_filtered = df.filter(df["redirect"].isNull())
        return await super()._filter(internal_filtered)

    async def _select(self, df: DataFrame) -> DataFrame:
        await asyncio.sleep(0)
        relevant_fields = df.select(
            df["title"], df["revision"]["text"]["_VALUE"].alias("text"), df["redirect"]
        )
        return relevant_fields

    async def _final_select(self, df: DataFrame) -> DataFrame:
        await asyncio.sleep(0)
        return df.select(df["title"], df["text"])
