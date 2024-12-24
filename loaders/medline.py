import os
from pyspark.sql import DataFrame, SparkSession
import asyncio

from .loader_utils import *
from .rag_datasource import *
from .medline_schema import medline_schema

class MedlineDataSource(RecursiveDataSource):
    flattern = True
    urls = [
        "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/",
        "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"
    ]
    input_options = {
        "rowTag": "PubmedArticle",
        "rootTag": "PubmedArticleSet",
    }
    schema = medline_schema
    input_format = "com.databricks.spark.xml"
    directory_name = "recursive_medline"
    match_condition = "*.xml.gz"

    async def _select(self, df: DataFrame) -> DataFrame:
        selected = df.select(
            df["MedlineCitation"]["Article"]["Abstract"]["AbstractText"]["_VALUE"].alias("medline_abstract_text_value"),
            df["MedlineCitation"]["PMID"].alias("medline_pmid"),
            df["MedlineCitation"]["Article"]["Journal"]["Title"].alias("medline_journal_title"),
            df["MedlineCitation"]["Article"]["ArticleTitle"].alias("medline_article_title"),
            df["MedlineCitation"]["NumberOfReferences"].alias("medline_num_refs"),
            df["MedlineCitation"]["OtherAbstract"].alias("medline_alt_abstract"),
            df["PubmedData"]["ArticleIdList"].alias("pubmed_article_ids"), # doi, pmedid
            df["PubmedData"]["PubmedPubDate"].alias("pubmed_pubdates")
        )
        selected.show()
        return selected
