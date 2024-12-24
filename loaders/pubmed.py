import os
from pyspark.sql import DataFrame, SparkSession
import asyncio

from .loader_utils import *
from .rag_datasource import *
from .pubmed_schema import pubmed_schema

class PubMedDataSource(RecursiveTgzDataSource):
    flattern = False
    directory_name = "recursive_pubmed_oa"
    urls = [
        "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/"
    ]
