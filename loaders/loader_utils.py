import gzip
import io
import tarfile
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

executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

s3_session = None
if minio_host:
    s3_session = aioboto3.Session(region_name=os.getenv("MINIO_REGION"))


def dl_local_or_minio_path(local_path: str) -> str:
    # Use local path if we are using a non-local master AND have minio setup.
    # This avoids doing double transfers of downloaded files if we don't need to
    if os.getenv("SPARK_MASTER") is not None and minio_host is not None and minio_bucket is not None:
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
        df.write.format("parquet").mode("overwrite").save(
            path
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

async def _compress_and_upload(client, s3_bucket, file_path, extracted_file, remote_path_compressed):
    """
    Compress and upload the file to S3 asynchronously.
    """
    try:
        # Make sure file is not laready there.
        await client.head_object(Bucket=s3_bucket, Key=str(remote_path_compressed))
    except:
        try:
            # Compress the file
            compressed_file = _sync_compress_file(file_obj)
            print(f"Uploading extracted file: {remote_path_compressed} from {file_path}")
        
            # Upload to S3 asynchronously
            await client.upload_fileobj(
                compressed_file,
                s3_bucket,
                remote_path_compressed
            )
            print(f"Uploaded compressed file: {remote_path_compressed}")
        except Exception as e:
            print(f"Error compressing and uploading {file_path}: {e}")
        finally:
            compressed_file.close()

def _sync_compress_file(file_obj):
    compressed_stream = BytesIO()
    with gzip.GzipFile(fileobj=compressed_stream, mode="wb") as gz:
        gz.write(file_obj.read())
    compressed_stream.seek(0)
    return compressed_stream

async def _delete_object(client, Bucket, Key):
    try:
        await client.delete_object(Bucket=Bucket, Key=Key)
        print(f"Successfully deleted {key}")
    except Exception as e:
        print(f"Failed to delete {key}: {e}")
            
async def _upload_file(file_path):
    # Check if we already have the file or upload it.
    async with create_s3_client() as client:
        try:
            print(f"Checking {file_path}")
            await client.head_object(Bucket=s3_bucket, Key=str(file_path))
        except Exception as e:
            print(f"Uploading {file_path}")
            await client.upload_file(file_path, s3_bucket, str(file_path))
            print(f"Uploaded {file_path}")
        # Is it a tarfile? If so upload the contents (Spark doesn't love loading from tars)
        if str(file_path).endswith(".tgz") or str(file_path).endswith(".tar.gz"):
            with tarfile.open(file_path, "r:gz") as tar:
                tasks = []
                for member in tar.getmembers():
                    if member.isfile():
                        # Get the relative path within the tar archive
                        relative_path = member.name
                        remote_path = f"{file_path}-extracted/{relative_path}"
                        remote_path_compressed = f"{file_path}-extracted/{relative_path}.gz"
                        # Non blocking delete the non-compressed version TODO:remove
                        asyncio.create_task(_delete_object(client, Bucket=s3_bucket, Key=str(remote_path)))
                        extracted_file = tar.extractfile(member)
                        print(f"Uploading extracted file: {member.name} from {file_path}")
                        tasks.append(_compress_and_upload(
                            client,
                            s3_bucket,
                            file_path,
                            extracted_file,
                            remote_path_compressed))
                await asyncio.gather(*tasks)

async def _upload_directory(directory: str):
    if s3_session is not None:
        tasks = []
        for file_path in pathlib.Path(directory).rglob('*'):
            if file_path.is_file():
                tasks.append(_upload_file(file_path))
        await asyncio.gather(*tasks)
    

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
