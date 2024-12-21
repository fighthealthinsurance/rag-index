from .pubmed import load_pubmed
from .arxiv import load_arxiv
from .wikipedia import load_wikipedia
import asyncio

async def magic():
    awaitables = [load_wikipedia(), load_arxiv(), load_pubmed()]
    main_bloop = asyncio.gather(*awaitables)
    await main_bloop

asyncio.run(magic())
