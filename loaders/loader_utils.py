import aioboto3
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

from .minio_settings import *

url_regex = r"(https?|ftp)://[a-zA-Z0-9.-]+(?:\.[a-zA-Z]{2,})+(/[^\s]*)?"
doi_regex = r"10\.\d{4,9}/[-._;()/:A-Z0-9]+"
semi_legit = "(nih.gov|Category:Nutrition|modernmedicine|PLOS Medicine|veterinaryevidence|Portal bar \|Medicine|World Health Organization|cihr-irsc.gc.ca|nihr.ac.uk|nhs.uk)"
semi_legit_compiled = re.compile(semi_legit, re.IGNORECASE)

executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

s3_session = None
if minio_host:
    s3_session = aioboto3.Session(region_name=os.getenv("MINIO_REGION"))


def local_or_minio_path(local_path: str) -> str:
    # Use local path if we are using a non-local master AND have minio setup.
    if os.getenv("SPARK_MASTER") is not None and minio_host is not None and minio_bucket is not None:
        return f"s3a://{minio_bucket}/local_path"
    else:
        return local_path


def create_s3_client():
    if (
        s3_session is not None
        and minio_username is not None
        and minio_password is not None
        and minio_host is not None
    ):

        return s3_session.client(
            service_name="s3",
            region_name=os.getenv("MINIO_REGION"),
            endpoint_url=os.getenv("MINIO_HOST"),
            aws_access_key_id=os.getenv("MINIO_USERNAME"),
            aws_secret_access_key=os.getenv("MINIO_PASSWORD"),
            aws_session_token=None,
            verify=False,
        )


s3_bucket = os.getenv("MINIO_BUCKET")


async def load_or_create(
    spark: SparkSession,
    input_path: str,
    create_fun: Callable[[SparkSession], Awaitable[DataFrame]],
) -> DataFrame:
    if input_path is None or len(input_path) == 0:
        raise Exception("Invalid input_path for load_or_create")
    bucket = os.getenv("MINIO_BUCKET")
    if bucket is not None:
        input_path = f"s3a://{bucket}/{input_path}"
    try:
        df = spark.read.parquet(input_path)
    except:
        # Unfortunately not meaningfuly async
        df = await create_fun(spark)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            executor, df.write.format("parquet").mode("overwrite").save, input_path
        )
    return df


async def check_call(cmd, max_retries=0, **kwargs):
    print(f"Running: {cmd}")
    process = await asyncio.create_subprocess_exec(
        *cmd, **kwargs, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    return_code = await process.wait()
    if return_code != 0:
        if max_retries < 1:
            raise CalledProcessError(return_code, cmd)
        else:
            print(f"Retrying {cmd}")
            return await check_call(cmd, max_retries=max_retries - 1, **kwargs)
    else:
        print(f"Success {cmd}")


async def download_file(target_file: str, urls: list[str]) -> None:
    if which("axel") is not None:
        cmd = ["axel", f"--output={target_file}"]
        cmd.extend(urls)
        await check_call(cmd)
    elif which("wget") is not None:
        await check_call(
            ["wget", f"--output-file={target_file}", urls[0]], max_retries=4
        )
    else:
        raise Exception("Need wget or axel installed.")


async def download_file_if_not_existing(target_file: str, urls: list[str]) -> None:
    """Download file if it does not exist OR does not pass checks."""
    target_file = f"Downloads/{target_file}"
    # Check MINIO first
    if s3_session is not None:
        async with create_s3_client() as client:
            try:
                await client.head_object(Bucket=s3_bucket, Key=target_file)
                return
            except Exception as e:
                print(f"{target_file} not in {s3_bucket} -- {e}")
    # Remove check validaty of local file
    if os.path.exists(target_file) and os.getenv("DEV") is None:
        await _check_or_remove_file(target_file)
    # Download it
    if not os.path.exists(target_file):
        await download_file(target_file, urls)
        # Make sure download is valid
        await _check_or_remove_file(target_file)
    # Upload to s3 if configured
    if s3_session is not None:
        async with create_s3_client() as client:
            print(f"Uploading {target_file}")
            await client.upload_file(target_file, s3_bucket, target_file)
            print(f"Done uploading {target_file}")


async def _download_recursive(directory: str, flatten: bool, url: str) -> None:
    directory = f"Downloads/{directory}"
    command = [
        "wget",
        "-nc",
        "-r",
        "-np",
        "-e",
        "robots=off",
        f"--directory-prefix=./{directory}",
    ]
    if flatten:
        command.append("-nd")
    command.append(url)
    await check_call(command, max_retries=5)
    # Upload to s3 if configured
    if s3_session is not None:
        async with create_s3_client() as client:
            for root, dirs, files in os.walk(directory):
                for file in files:
                    file_path = os.path.join(directory, file)
                    # Check if we already have the file or upload it.
                    try:
                        await client.head_object(Bucket=s3_bucket, Key=file_path)
                    except Exception as e:
                        await client.upload_file(file_path, s3_bucket, file_path)
    return None


async def _check_or_remove_file(target_file: str) -> None:
    try:
        path = pathlib.Path(target_file)
        if which("gunzip") is not None and (
            path.suffix == ".gz" or path.suffix == ".tgz"
        ):
            await check_call(["gunzip", "-t", target_file])
        elif which("bzip2") is not None and (
            path.suffix == ".bz2" or path.suffix == ".tbz2"
        ):
            await check_call(["bzip2", "-t", target_file])
        elif which("unzip") is not None and path.suffix == ".zip":
            await check_call(["unzip", "-t", target_file])
    except subprocess.CalledProcessError:
        os.remove(target_file)
    return


async def _check_directory(directory: str) -> AsyncGenerator[str, None]:
    for path in pathlib.Path(directory).rglob("*"):
        if path.is_file():
            # Remove invalid files
            target_file = path.as_posix()
            await _check_or_remove_file(target_file)
            yield target_file


async def check_directory(directory: str) -> None:
    if os.getenv("DEV") is not None:
        return
    tasks = [
        _check_or_remove_file(file)
        async for file in _check_directory(directory)
    ]
    await asyncio.gather(*tasks)


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
    relevant_records = df.filter(df["text"].rlike(semi_legit))
    return relevant_records


def extract_and_annotate(df: DataFrame) -> DataFrame:
    """Extract and annotate LINKs and DOIs."""
    extracted = df.withColumn(
        "extracted_urls", regexp_extract_all("text", lit(url_regex), lit(0))
    ).withColumn("dois", regexp_extract_all("text", lit(doi_regex), lit(0)))
    return extracted
