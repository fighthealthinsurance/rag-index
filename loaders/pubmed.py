import os
from pyspark.sql import DataFrame, SparkSession
import asyncio

from .loader_utils import *
from .rag_datasource import *

class PubMedDataSource(RecursiveDataSource):
    flattern = True
    urls = [
        "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/",
        "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"
    ]
    options = {"rowTag": "PubMedArticle"}
    input_format = "xml"
    directory_name = "recursive_pubmed"
    match_condition = "pubmed24n1314.xml.gz"
