import os

from .loader_utils import *

async def download_pubmed():
    await download_recursive([
        "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/",
        "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"
    ])

async def load_pubmed():
#    await download_pubmed
    return
