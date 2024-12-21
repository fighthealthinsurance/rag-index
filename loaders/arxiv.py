import os

from .loader_utils import *

arxiv_file = "arxiv.zip"

async def download_arxiv():
    urls = ["https://www.kaggle.com/api/v1/datasets/download/Cornell-University/arxiv"]
    return await download_file_if_not_existing(arxiv_file, urls)

async def load_arxiv():
#    await download_arxiv
    return

    
