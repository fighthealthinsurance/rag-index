import asyncio
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    input_file_name,
    regexp_replace,
    split,
    udf,
    element_at
)
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, explode


from .loader_utils import *
from .rag_datasource import *

@udf
def extract_journal_title_udf(x) -> str:
    return str(x)

@udf
def extract_article_title_udf(x) -> str:
    return str(x)

class PubMedDataSource(RecursiveTgzDataSource):
    name = "pubmed"
    flattern = False
    input_options = {
        "rowTag": "article",
    }
    # match the filelist csvs
    match_condition = "ftp.ncbi.nlm.nih.gov/pub/pmc/oa_*/*/*/*/*/*.xml*"
    input_format = "com.databricks.spark.xml"
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
        selected = df.select(
            df["front"]["journal-meta"]["journal-id"].alias("journal_ids"),
            extract_journal_title_udf(df["front"]["journal-meta"]["journal-title-group"]["journal-title"]).alias("journal_titles"),
            extract_article_title_udf(df["front"]["article-meta"]["title-group"]["article-title"]).alias("article_title"),
            explode(df["front"]["article-meta"]["article-id"]).alias("exploded"),
            flatten_text_udf(df["front"]["article-meta"]["abstract"]).alias("abstract"),
            flatten_text_udf(df["body"]).alias("text")
        )
        withpubmed_ids = selected.filter(
            selected["exploded"]["_pub-id-type"] == "pmid").select(
                "journal_ids",
                "journal_titles",
                "article_title",
                "abstract",
                "text",
                selected["exploded"]["_VALUE"].alias("pubmed_id")
            )
        withpubmed_ids.show()
        return withpubmed_ids

    async def _filter(self, df: DataFrame) -> DataFrame:
        return df

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
        df.show(truncate=True)
        return df
