import asyncio
import json
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from .loader_utils import *
from .rag_datasource import *


class ArxivDataSource(CompressedRagDataSource):
    target_partitions: int = 10
    name = "arxiv"
    filename = "arxiv.zip"
    extracted_filename = "arxiv-metadata-oai-snapshot.json"
    input_format = "json"
    decompress_needed = True
    schema = StructType(
        [
            StructField("abstract", StringType(), True),
            StructField("authors", StringType(), True),
            StructField(
                "authors_parsed", ArrayType(ArrayType(StringType(), True), True), True
            ),
            StructField("categories", StringType(), True),
            StructField("comments", StringType(), True),
            StructField("doi", StringType(), True),
            StructField("id", StringType(), True),
            StructField("journal-ref", StringType(), True),
            StructField("license", StringType(), True),
            StructField("report-no", StringType(), True),
            StructField("submitter", StringType(), True),
            StructField("title", StringType(), True),
            StructField("update_date", StringType(), True),
            StructField(
                "versions",
                ArrayType(
                    StructType(
                        [
                            StructField("created", StringType(), True),
                            StructField("version", StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )

    @property
    def urls(self):
        return [
            "https://www.kaggle.com/api/v1/datasets/download/Cornell-University/arxiv"
        ]

    async def _filter(self, df: DataFrame) -> DataFrame:
        category_filtered = df.filter(df["categories"].contains("bio"))
        upstream_filtered = await super()._filter(category_filtered)
        return upstream_filtered

    async def _extract(self):
        if not os.path.exists(f"Downloads/{self.extracted_filename}"):
            await check_call(["unzip", f"Downloads/{self.filename}", "-d", "Downloads"])

    async def _select(self, df: DataFrame) -> DataFrame:
        """Select the relevant fields from an ARXIV record."""
        initial = df
        relevant_fields = initial.select(
            initial["doi"],
            initial["title"],
            initial["authors"],
            initial["authors_parsed"],
            initial["categories"],
            initial["update_date"],
            initial["abstract"].alias("text"),
            initial["id"].alias("arxiv_id"),
        )
        return relevant_fields
