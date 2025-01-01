import asyncio
import concurrent
import gzip
import io
import os
import pathlib
import random
import re
import subprocess
import tarfile
import tempfile
from shutil import disk_usage, which
from subprocess import CalledProcessError
from typing import AsyncGenerator, Awaitable, Callable, Generator, List, Optional

import aioboto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, regexp, regexp_extract_all

from .minio_settings import *

url_regex = r"(https?|ftp)://[a-zA-Z0-9.-]+(?:\.[a-zA-Z]{2,})+(/[^\s]*)?"
doi_regex = r"10\.\d{4,9}/[-._;()/:A-Z0-9]+"
semi_legit = "(nih\\.gov|Category:Nutrition|modernmedicine|PLOS Medicine|Portal bar ..Medicine|World Health Organization|https?://[a-z\.A-Z]*cihr-irsc\\.gc\\.ca|https?://[a-z\.A-Z]*nihr\\.ac\\.uk|nhs\\.uk)"
semi_legit_compiled = re.compile(semi_legit, re.IGNORECASE)

executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

mini_pipeline = False
tmp = os.getenv("TESTING_MINI_PIPELINE")
if tmp:
    if tmp.lower() == "true":
        mini_pipeline = True

s3_session = None
if minio_host:
    s3_session = aioboto3.Session(region_name=os.getenv("MINIO_REGION"))


def dl_local_or_minio_path(local_path: str) -> str:
    # Use local path if we are using a non-local master AND have minio setup.
    # This avoids doing double transfers of downloaded files if we don't need to
    if (
        os.getenv("SPARK_MASTER") is not None
        and minio_host is not None
        and minio_bucket is not None
    ):
        return f"s3a://{minio_bucket}/{local_path}"
    else:
        return local_path


def local_or_minio_path(local_path: str) -> str:
    # Use local path if we are using have minio setup. Always transfers to minio.
    if minio_host is not None and minio_bucket is not None:
        return f"s3a://{minio_bucket}/{local_path}"
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
    path = local_or_minio_path(input_path)
    try:
        df = spark.read.parquet(path)
    except:
        # Unfortunately not meaningfuly async
        df = await create_fun(spark)
        df.write.format("parquet").mode("overwrite").save(path)
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
    await asyncio.sleep(0)
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
            await asyncio.sleep(0)
            await client.upload_file(target_file, s3_bucket, target_file)
            print(f"Done uploading {target_file}")


async def _delete_object(client, Bucket, Key):
    try:
        await asyncio.sleep(random.randint(0, 3))
        await client.delete_object(Bucket=Bucket, Key=Key)
    except Exception as e:
        pass


async def _upload_file(
    file_path: pathlib.Path, target: Optional[str] = None, delete=False, max_retries=3
):
    try:
        # Check if we already have the file or upload it.
        # Perf is shit but we run this infrequently and I'm lazy.
        async with create_s3_client() as client:
            tasks = []
            _target = str(file_path)
            if target is not None:
                _target = target
            try:
                await asyncio.sleep(random.randint(0, 3))
                await client.head_object(Bucket=s3_bucket, Key=_target)
            except Exception as e:
                tasks.append(client.upload_file(file_path, s3_bucket, _target))
            # Is it a tarfile? If so upload the contents (Spark doesn't love loading from tars)
            if str(file_path).endswith(".tgz") or str(file_path).endswith(".tar.gz"):
                # Wait a random amount of time before starting our magic
                await asyncio.sleep(random.randint(0, 30))
                with tempfile.TemporaryDirectory(
                    prefix="extract" + file_path.name[0:20]
                ) as extract_dir:
                    # Check that we have a lot of free space
                    usage = disk_usage("/tmp")
                    delay = 10
                    while usage.free / usage.total < 0.38:
                        usage = disk_usage("/tmp")
                        delay = (
                            10
                            + int((usage.total / usage.free) * 10)
                            + random.randint(0, 10)
                        )
                        print(
                            f"Running low on space {usage}, waiting {delay} + {tasks}..."
                        )
                        usage = disk_usage("/tmp")
                        await asyncio.gather(*tasks)
                        tasks = []
                        await asyncio.sleep(delay)
                    # Extract archive better than blocking the Python thread
                    await check_call(["tar", "-xf", str(file_path), "-C", extract_dir])
                    for extracted_file_path in pathlib.Path(extract_dir).rglob("*"):
                        # Only upload files and not internally compressed files
                        if extracted_file_path.is_file() and not str(
                            extracted_file_path
                        ).endswith(".gz"):
                            relative_path = str(extracted_file_path).lstrip(
                                extract_dir + "/"
                            )
                            remote_path = f"{file_path}-extracted/{relative_path}"
                            remote_path_compressed = (
                                f"{file_path}-extracted/{relative_path}.gz"
                            )
                            # Do we need to upload this file?
                            try:
                                await client.head_object(
                                    Bucket=s3_bucket, Key=str(remote_path_compressed)
                                )
                                extracted_file_path.unlink()
                            except:
                                # Again avoid using the Python gzip lib for perf
                                await check_call(["gzip", str(extracted_file_path)])
                                # Non blocking fire and forget delete decompressed remote object
                                asyncio.create_task(
                                    _delete_object(
                                        client, Bucket=s3_bucket, Key=str(remote_path)
                                    )
                                )
                                tasks.append(
                                    _upload_file(
                                        pathlib.Path(f"{str(extracted_file_path)}.gz"),
                                        target=remote_path_compressed,
                                        delete=True,
                                    )
                                )
                            # Avoid staking too many tasks. 100 is arb.
                            if len(tasks) > 100:
                                await asyncio.gather(*tasks)
                                tasks = []
                    # await here before we delete the temp dir
                    await asyncio.gather(*tasks)
                    tasks = []
            # Incase no tgz await
            await asyncio.gather(*tasks)
            if delete:
                file_path.unlink()
    except Exception as e:
        if max_retries > 1:
            print(f"Error {e} waiting before we retry")
            await asyncio.sleep(random.randint(0, 120))
            if isinstance(e, OSError):
                print(f"Was an OSError... waiting a bit more")
                await asyncio.sleep(random.randint(60, 300))
            print("Retrying!")
            await _upload_file(
                file_path, delete=delete, target=target, max_retries=max_retries - 1
            )
        else:
            raise e


async def _upload_directory(directory: str, max_retries=2):
    if s3_session is not None:
        tasks = []
        for file_path in pathlib.Path(directory).rglob("*"):
            if file_path.is_file():
                tasks.append(_upload_file(file_path))
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            await asyncio.sleep(random.randint(60, 300))
            if max_retries > 0:
                await _upload_directory(directory, max_retries - 1)
            else:
                raise e
    # We can also just do local extractions for mini mode
    elif mini_pipeline:
        for file_path in pathlib.Path(directory).rglob("*"):
            if file_path.is_file():
                if file_path.endswith(".tar.gz"):
                    extract_dir = str(file_path) + "-extract"
                    await check_call(["tar", "-xf", str(file_path), "-C", extract_dir])
                elif file_path.endswith(".gz"):
                    await check_call("gunzip", str(file_path))


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
    await _upload_directory(directory)
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
    tasks = [_check_or_remove_file(file) async for file in _check_directory(directory)]
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
