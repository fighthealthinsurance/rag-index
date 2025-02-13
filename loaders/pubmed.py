import asyncio
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    input_file_name,
    regexp_replace,
    split,
    udf,
    element_at,
)
from pyspark.sql.types import StringType

from .loader_utils import *
from .rag_datasource import *


class PubMedDataSource(RecursiveTgzDataSource):
    name = "pubmed"
    flattern = False
    input_options = {
        "recursiveFileLookup": "True",
        "header": "True",
    }
    # match the filelist csvs
    match_condition = "ftp.ncbi.nlm.nih.gov/pub/pmc/oa_*/*/*/*.filelist.csv"
    input_format = "csv"
    directory_name = "recursive_pubmed_oa"
    target_partitions = 10000

    @property
    def urls(self) -> list[str]:
        if mini_pipeline:
            return [
                "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml/oa_comm_xml.incr.2025-01-01.filelist.csv",
                "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml/oa_comm_xml.incr.2025-01-01.tar.gz",
            ]
        return ["https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml/",
                ]

    async def _select(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("file_name", split(df["Article File"], "/").getItem(1))
            .withColumn("csv_input_file_name", input_file_name())
            .withColumn(
                "artifact_file_path",
                regexp_replace(input_file_name(), "\\.filelist\\.csv$", ".tar.gz"),
            )
        )

    async def _filter(self, df: DataFrame) -> DataFrame:
        return df.filter(df["Retracted"] != "yes")

    async def _extract(self):
        if mini_pipeline:
            # Static extract so as we add files we don't reset the glob
            file_paths = list(
                pathlib.Path(f"Downloads/{self.directory_name}").rglob("*.tar.gz")
            )
            print(
                f"Extracting from {file_paths} out of Downloads/{self.directory_name}"
            )
            for file_path in file_paths:
                if file_path.is_file():
                    if str(file_path).endswith(".tar.gz"):
                        extract_dir = str(file_path) + "-extract"
                        os.makedirs(extract_dir, exist_ok=True)
                        await check_call(
                            [
                                "tar",
                                "--skip-old-files",
                                "-xf",
                                str(file_path),
                                "-C",
                                extract_dir,
                            ]
                        )
                    else:
                        print(f"Skipping {file_path}")

    async def _final_select(self, df: DataFrame) -> DataFrame:
        # Here we're a little different since we're doing a join.
        # Always read from S3A _unless_ we're in miniepipeline mode
        raise Exception("Nope")
        path = f"s3a://{minio_bucket}/Downloads/recursive_pubmed_oa/ftp.ncbi.nlm.nih.gov/pub/pmc/oa_*/*/*/*/*/*.xml"
        if mini_pipeline:
            path = "./Downloads/recursive_pubmed_oa/ftp.ncbi.nlm.nih.gov/pub/pmc/oa_*/*/*/*/*/*.xml"
        spark = SparkSession.builder.getOrCreate()
        text_files = (
            spark.read.format("text")
            .option("wholeText", "True")
            .load(path)
            .withColumn("txt_input_file_name", input_file_name())
            .withColumn("file_name", element_at(split(input_file_name(), "/"), -1))
            .withColumnRenamed("value", "text")
        )
        df.show(truncate=True)
        text_files.show(truncate=True)
        joined = df.join(text_files, on="file_name")
        return joined
