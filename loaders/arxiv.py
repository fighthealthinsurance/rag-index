import asyncio
import os
import json
from pyspark.sql import DataFrame, SparkSession

from .loader_utils import *

class ArxivDataSource(CompressedRagDataSource):
    filename = "arxiv.zip"
    extracted_filename = "arxiv-metadata-oai-snapshot.json"
    input_format = "json"
    urls = ["https://www.kaggle.com/api/v1/datasets/download/Cornell-University/arxiv"]
    extract_command = "unzip"

    async def _select(initial: DataFrame) -> DataFrame:
        """Select the relevant fields from an ARXIV record."""
        relevant_fields = initial.select(
            initial["doi"],
            initial["title"],
            initial["abstract"].alias("text"),
            initial["id"].alias("arxiv_id"))
        return relevant_fields
