from shutil import which
import os
import subprocess
import asyncio
from typing import List

from subprocess import CalledProcessError

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
