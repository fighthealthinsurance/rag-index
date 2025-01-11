import asyncio
import os
from typing import Dict, List, Optional
import subprocess

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType

from .loader_utils import *


class RagDataSource:
    name: str = ""
    target_partitions: int = 100
    loader_options: Dict[str, str] = {"fake": "option"}
    input_format: str = "parquet"
    schema: Optional[StructType] = None
    input_options: Dict[str, str] = {}

    async def _download(self, spark: SparkSession) -> None:
        """Download the data. Spark session provided to upload to cluster if needed."""
        return

    def path(self) -> str:
        raise NotImplementedError("Must implement path")

    async def _extract(self):
        return

    async def _initial_load(self, spark: SparkSession) -> DataFrame:
        """Async load function."""
        await asyncio.sleep(0)
        read_call = spark.read.format(self.input_format)
        if self.schema is not None:
            read_call = read_call.schema(self.schema)
        for k, v in self.input_options.items():
            read_call = read_call.options(**{k: v})
        path = self.path()
        loaded = read_call.load(dl_local_or_minio_path(path))
        print(f"Loaded {path} w/schema {loaded.schema} & columns {loaded.columns}")
        loaded.show()
        return loaded

    async def _filter(self, df: DataFrame) -> DataFrame:
        """Trivial filter, accept all records."""
        return df

    async def _select(self, df: DataFrame) -> DataFrame:
        """Default select: keep all of the columns. applied pre-filter"""
        return df

    async def _final_select(self, df: DataFrame) -> DataFrame:
        """Final select (applied post filter)"""
        return df

    async def _load(self, spark: SparkSession) -> DataFrame:
        print(f"Downloading {self.name}")
        await self._download(spark)
        await asyncio.sleep(1)
        subprocess.run(["sync"])
        await asyncio.sleep(1)
        print(f"Extracting {self.name}")
        await self._extract()
        await asyncio.sleep(1)
        subprocess.run(["sync"])
        await asyncio.sleep(1)
        print(f"Loading {self.name}")
        df = await self._initial_load(spark)
        selected = await self._select(df)
        filtered = (await self._filter(selected)).repartition(self.target_partitions)
        final_selected = await self._final_select(filtered)
        return await self._annotate(final_selected)

    async def _annotate(self, df: DataFrame) -> DataFrame:
        await asyncio.sleep(0)
        annotated = extract_and_annotate(df).repartition(self.target_partitions)
        return annotated.withColumn("data_source", lit(self.name))

    async def load(self, spark: SparkSession) -> DataFrame:
        await asyncio.sleep(0)
        return await load_or_create(spark, self.name, self._load)


class RecursiveDataSource(RagDataSource):
    directory_name: str = ""
    input_format: str = "json"
    schema: Optional[StructType] = None
    match_condition: str = "*"
    flatten = True

    async def _download(self, spark: SparkSession):
        """Download the data."""
        await asyncio.sleep(0)
        await download_recursive(self.directory_name, self.flatten, self.urls)

    def path(self) -> str:
        return dl_local_or_minio_path(
            f"Downloads/{self.directory_name}/{self.match_condition}"
        )

    @property
    def urls(self) -> list[str]:
        return []


class RecursiveTgzDataSource(RecursiveDataSource):
    directory_name: str = ""
    input_format: str = "json"
    schema: Optional[StructType] = None
    match_condition: str = "*"
    flatten = False


class CompressedRagDataSource(RagDataSource):
    filename: str = ""
    extracted_filename: str = ""
    input_format: str = "csv"
    schema: Optional[StructType] = None
    input_options: Dict[str, str] = {}
    decompress_needed = False

    @property
    def urls(self) -> list[str]:
        return []

    async def _download(self, spark: SparkSession):
        """Download the data."""
        await asyncio.sleep(0)
        return await download_file_if_not_existing(self.filename, self.urls)

    def path(self) -> str:
        if self.decompress_needed:
            return dl_local_or_minio_path(f"Downloads/{self.extracted_filename}")
        else:
            return dl_local_or_minio_path(f"Downloads/{self.filename}")

    async def _select(self, df: DataFrame) -> DataFrame:
        """Select the relevant fields, most likely should be overridden."""
        return df

    async def _filter(self, df: DataFrame) -> DataFrame:
        return self.filter_relevant_records_based_on_text(df)

    def filter_relevant_records_based_on_text(self, df: DataFrame) -> DataFrame:
        """Filter relevant records (placeholder implementation)."""
        return df
