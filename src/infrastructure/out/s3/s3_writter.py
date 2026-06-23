from typing import Literal

import awswrangler as wr
import pandas as pd


def write_parquet_dataset(
        df: pd.DataFrame,
        path: str,
        mode: Literal["append", "overwrite", "overwrite_partitions"] | None = "append",
        partition_cols: list[str] | None = None,
) -> None:
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode=mode,
        partition_cols=partition_cols or [],
    )