import re
from shutil import which
import os
import subprocess
import asyncio
from typing import List
from pyspark.sql import DataFrame, SparkSession
from typing import Callable
import concurrent

from subprocess import CalledProcessError

url_regex = r'(https?://[^\s]+|www\.[^\s]+)'
doi_regex = r'10\.\d{4,9}/[-._;()/:A-Z0-9]+'
semi_legit = "(nih.gov|Category:Nutrition|modernmedicine|PLOS Medicine|veterinaryevidence|Portal bar \|Medicine|World Health Organization|cihr-irsc.gc.ca|nihr.ac.uk|nhs.uk)"
semi_legit_compiled = re.compile(
    semi_legit,
    re.IGNORECASE)

def load_or_create(
        spark: SparkSession,
        input_path: str,
        create_fun: Callable[[SparkSession], DataFrame]) -> DataFrame:
    try:
        df = spark.load(input_path)
    except:
        df = create_fun(spark)
        df.save(input_path)
    return df

async def check_call(cmd, **kwargs):
    process = await asyncio.create_subprocess_exec(*cmd, **kwargs)
    return_code = await process.wait()
    if return_code != 0:
        raise CalledProcessError(return_code, cmd)

async def download_file(target_file: str, urls: list[str]) -> None:
    if which("axel") is not None:
        cmd = ["axel", f"--output={target_file}"]
        cmd.extend(urls)
        await check_call(cmd)
    elif which("wget") is not None:
        await check_call(["wget", f"--output-file={target_file}", urls[0]])
    else:
        raise Exception("Need wget or axel installed.")

async def download_file_if_not_existing(target_file: str, urls: list[str]) -> None:
    """Download file if it does not exist OR does not pass checks."""
    if os.path.exists(target_file):
        if target_file.endswith(".zip") and which("unzip") is not None:
            try:
                await check_call(["unzip", "-t", target_file])
                return
            except Exception as e:
                print(f"Error with file integrity check {target_file}")
                await check_call(["rm", target_file])
        elif target_file.endswith(".bz2") and which("bzip2") is not None:
            try:
                await check_call(["bzip2", "-t", target_file])
                return
            except Exception as e:
                print(f"Error with file integrity check {target_file}")
                await check_call(["rm", target_file])
    await download_file(target_file, urls)
        

async def _download_recursive(url: str) -> None:
    await check_call(["wget", "-nc", "-r", "-nd", "--level=1", "-np", "-e", "robots=off", "--directory-prefix=./recursive", url])
    return None

async def download_recursive(urls: list[str]) -> None:
    awaitables = map(_download_recursive, urls)
    await asyncio.gather(*awaitables)

def is_maybe_relevant(document_text: str) -> bool:
    if semi_legit.match(document_text):
        return True
    else:
        return False

def filter_relevant_records_based_on_text(df: DataFrame) -> DataFrame:
    relevant_records = df.filter(
        regexp(
            relevant_fields["text"],
            lit(semi_legit)))
    return relevant_records

def extract_and_annotate(df: DataFrame) -> DataFrame:
    """Extract and annotate LINKs and DOIs."""
    extracted = df.withColumn(
        "extracted_urls", regexp_extract_all("text", lit(url_regex))
    ).withColumn(
        "dois", regexp_extract_all("text", lit(doi_regex))
    )

def run_in_thread(func, *args):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        return executor.submit(func, *args).result()
