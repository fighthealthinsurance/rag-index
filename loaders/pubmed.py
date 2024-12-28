import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import input_file_name, split, regexp_replace, udf
from pyspark.sql.types import StringType
import asyncio

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
    match_condition = "ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/*/*/*.filelist.csv"
    input_format = "csv"
    directory_name = "recursive_pubmed_oa"
    urls = ["https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/"]
    target_partitions = 10000

    async def _select(self, df: DataFrame) -> DataFrame:
        return df.withColumn("file_name", split(df["Article File"], "/").getItem(1)) \
              .withColumn("input_file_name", input_file_name()) \
              .withColumn("artifact_file_path",
                          regexp_replace(input_file_name(), "\\.filelist\\.csv$", ".tar.gz"))

    async def _filter(self, df: DataFrame) -> DataFrame:
        return df.filter(df["Retracted"] != "yes")

    async def _final_select(self, df: DataFrame) -> DataFrame:
        # Load the files dynamically -- maybe move to Java idk.
        def load_file(artifact_path: str, filename: str):
            import tarfile
            # Use fsspec so we can have local or remote (note: configuration tbd)
            import fsspec

            with fsspec.open(artifact_path, mode="rb") as artifact_loaded:
                with tarfile.open(fileobj=artifact_loaded, mode="r:gz") as tar:
                    f = tar.extract(filename)
                    return f.read()

        load_file_udf = udf(load_file, StringType())
        r = df \
            .withColumn("loaded_content",
                        load_file_udf("artifact_file_path", "file_name"))
        r.show(truncate=False)
        return r
