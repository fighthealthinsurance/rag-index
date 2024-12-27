import re
from shutil import which
import os
import subprocess
import asyncio
from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import regexp_extract_all, lit, regexp
from typing import Awaitable, AsyncGenerator, Callable, Generator
import concurrent
import pathlib

from subprocess import CalledProcessError

url_regex = r'(https?|ftp)://[a-zA-Z0-9.-]+(?:\.[a-zA-Z]{2,})+(/[^\s]*)?'
doi_regex = r'10\.\d{4,9}/[-._;()/:A-Z0-9]+'
semi_legit = "(nih.gov|Category:Nutrition|modernmedicine|PLOS Medicine|veterinaryevidence|Portal bar \|Medicine|World Health Organization|cihr-irsc.gc.ca|nihr.ac.uk|nhs.uk)"
semi_legit_compiled = re.compile(
    semi_legit,
    re.IGNORECASE)

executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

async def load_or_create(
        spark: SparkSession,
        input_path: str,
        create_fun: Callable[[SparkSession], Awaitable[DataFrame]]) -> DataFrame:
    if input_path is None or len(input_path) == 0:
        raise Exception("Invalid input_path for load_or_create")
    try:
        df = spark.read.parquet(input_path)
    except:
        # Unfortunately not meaningfuly async
        df = await create_fun(spark)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            executor,
            df.write.format("parquet").mode("overwrite").save,
            input_path)
    return df

async def check_call(cmd, max_retries=0, **kwargs):
    print(f"Running: {cmd}")
    process = await asyncio.create_subprocess_exec(*cmd, **kwargs)
    return_code = await process.wait()
    if return_code != 0:
        if max_retries < 1:
            raise CalledProcessError(return_code, cmd)
        else:
            return await check_call(cmd, max_retries = max_retries - 1, **kwargs)

async def download_file(target_file: str, urls: list[str]) -> None:
    if which("axel") is not None:
        cmd = ["axel", f"--output={target_file}"]
        cmd.extend(urls)
        await check_call(cmd)
    elif which("wget") is not None:
        await check_call(["wget", f"--output-file={target_file}", urls[0]], max_retries=4)
    else:
        raise Exception("Need wget or axel installed.")

async def download_file_if_not_existing(target_file: str, urls: list[str]) -> None:
    """Download file if it does not exist OR does not pass checks."""
    # Remove invalid file
    if os.path.exists(target_file) and os.getenv("DEV") is None:
        await _check_or_remove_file(target_file)
    if not os.path.exists(target_file):
        await download_file(target_file, urls)


async def _download_recursive(directory: str, flatten: bool, url: str) -> None:
    command = ["wget", "-nc", "-r", "-np", "-e", "robots=off", f"--directory-prefix=./{directory}"]
    if flatten:
        command.append("-nd")
    command.append(url)
    await check_call(command, max_retries=5)
    return None

async def _check_or_remove_file(target_file: str) -> None:
    try:
        path = pathlib.Path(target_file)
        if which("gunzip") is not None and (path.suffix == ".gz" or path.suffix == ".tgz"):
            await check_call(["gunzip", "-t", target_file])
        elif which("bzip2") is not None and (path.suffix == ".bz2" or path.suffix == ".tbz2"):
            await check_call(["bzip2", "-t", target_file])
        elif which("unzip") is not None and path.suffix == ".zip":
            await check_call(["unzip", "-t", target_file])
    except subprocess.CalledProcessError:
        os.remove(target_file)
    return


async def _check_directory(directory: str) -> AsyncGenerator[None, None]:
    for path in pathlib.Path(directory).rglob("*"):
        if path.is_file():
            # Remove invalid files
            target_file = path.as_posix()
            yield _check_or_remove_file(target_file)

async def check_directory(directory: str) -> None:
    if os.getenv("DEV") is not None:
        return
    await asyncio.gather(*[task async for task in _check_directory(directory)])

async def download_recursive(directory: str, flatten: bool, urls: list[str]) -> None:
    await check_directory(directory)
    awaitables = map(lambda url: _download_recursive(directory, flatten, url), urls)
    await asyncio.gather(*awaitables)

def is_maybe_relevant(document_text: str) -> bool:
    if semi_legit_compiled.match(document_text):
        return True
    else:
        return False

def filter_relevant_records_based_on_text(df: DataFrame) -> DataFrame:
    relevant_records = df.filter(
        df["text"].rlike(semi_legit))
    return relevant_records

def extract_and_annotate(df: DataFrame) -> DataFrame:
    """Extract and annotate LINKs and DOIs."""
    extracted = df.withColumn(
        "extracted_urls", regexp_extract_all("text", lit(url_regex), lit(0))
    ).withColumn(
        "dois", regexp_extract_all("text", lit(doi_regex), lit(0))
    )
    return extracted
