import logging
from dataclasses import asdict
from datetime import datetime, timezone

import pandas as pd

from domain.extract.extract_raw_model import extract_raw_model_row
from infrastructure.hf_client import build_hf_api
from infrastructure.io.s3_writer import write_parquet_dataset
from infrastructure.secrets import get_hf_token


logger = logging.getLogger(__name__)


def run_raw_ingestion(config) -> None:
    logger.info("Starting raw ingestion job")

    token = get_hf_token(
        secret_name=config.hf_secret_name,
        region_name=config.region,
        fallback_token=getattr(config, "hf_token", None),
    )

    api = build_hf_api(token=token)

    logger.info("Requesting model list from Hugging Face")
    models = api.list_models(cardData=True)

    rows = []
    for model in models:
        row = extract_raw_model_row(model)
        if row is not None:
            rows.append(row)

    df = pd.DataFrame(rows)

    if df.empty:
        logger.warning("No models with CO2 emissions were found")
    else:
        logger.info("Extracted %s raw rows", len(df))

    df["snapshot_date"] = config.run_date
    df["ingested_at_utc"] = datetime.now(timezone.utc)

    write_parquet_dataset(
        df=df,
        path=config.bronze_path,
        mode="overwrite" if config.full_refresh else "append",
        partition_cols=["snapshot_date"],
    )

    logger.info("Raw ingestion completed successfully")