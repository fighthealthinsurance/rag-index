import asyncio
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import concat_ws

from .loader_utils import *
from .medline_schema import medline_schema
from .rag_datasource import *


class LegacyCDC(CompressedRagDataSource):
    flattern = True
    schema = medline_schema
    input_format = "text"
    input_options = {"wholetext": "true"}
    directory_name = "legacy_cdc"
    name = "legacy_pre_feb2025_cdc"
    decompress_needed = True  # zip of textfiles
    extracted_dir = oivey2025_cdc

    @property
    def urls(self) -> list[str]:
        return [
            "https://archive.org/compress/20250128-cdc-datasets/formats=DJVUTXT&file=/20250128-cdc-datasets.zip"
        ]

    async def _extract(self):
        if not os.path.exists(f"Downloads/{self.extracted_dir}"):
            await check_call(["mkdir", "-p", f"Downloads/{self.extracted_dir}"])
            await check_call(
                [
                    "unzip",
                    f"Downloads/{self.filename}",
                    "-d",
                    f"Downloads/{self.extracted_dir}",
                ]
            )

    async def _select(self, df: DataFrame) -> DataFrame:
        await asyncio.sleep(0)
        print(f"Selecting the relevant components...")
        selected = df.select(
            df["value"].alias("text"),
            input_file_name().alias("cdc_filename"),
        )
        print("Loaded the medline data:")
        selected.show()
        print("Huzzah!")
        return selected
