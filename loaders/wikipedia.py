import re
import os
from lxml import etree
import mwxml
import asyncio

from .loader_utils import *

wiki_date = f"20241201"
wiki_multistream_filename = f"enwiki-{wiki_date}-pages-articles-multistream.xml.bz2"

semi_legit = re.compile(
    "(nih.gov|Category:Nutrition|modernmedicine|PLOS Medicine|veterinaryevidence|Portal bar \| Medicine|World Health Organization)",
    re.IGNORECASE)

async def download_wikipedia():
    wikimedia_mirrors = [
        f"https://dumps.wikimedia.org/enwiki/{wiki_date}/",
        f"https://dumps.wikimedia.your.org/enwiki/{wiki_date}/"]
    urls = list(
        map(lambda base: f"{base}{wiki_multistream_filename}",
            wikimedia_mirrors))
    await download_file_if_not_existing(wiki_multistream_filename, urls)

def stream_wikipedia():
    import bz2

    with bz2.open(wiki_multistream_filename, "rb") as f:
        wikipedia_itr = mwxml.Dump.from_file(f)
        for page in wikipedia_itr:
            try:
                top_revision = next(page)
                text_deleted = top_revision.deleted.text
                if text_deleted:
                    continue
                page_text = top_revision.text
                if page_text is None:
                    continue
                page_title = top_revision.page.title
                if semi_legit.match(page_text):
                    yield {
                        "text": page_text,
                        "title": page_title
                    }
            except Exception as e:
                print(f"Error: {e} -- skipping")
                continue

async def load_wikipedia():
#    await download_wikipedia()
    article_stream = stream_wikipedia()
    for article in article_stream:
        await asyncio.sleep(0) # yield
        print(article)
        return
    return
