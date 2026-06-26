import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True,
)

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from raw_ingestion.infrastructure.into.glue.config.injection.recuperation_dependency_injector import (
    build_data_recuperation_service,
)
from raw_ingestion.infrastructure.into.glue.config.recuperation_config import load_recuperation_config


def main() -> None:
    config = load_recuperation_config()

    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(config.job_name, {})

    logging.info(
        "data_recuperation job started: run_id=%s, source_bucket=%s",
        config.run_id,
        config.source_bucket,
    )

    try:
        service = build_data_recuperation_service(
            table_name=config.control_table_name,
        )
        metrics = service.run(
            run_id=config.run_id,
            source_bucket=config.source_bucket,
        )
        logging.info(
            "data_recuperation job completed: recuperated=%d, still_failed=%d, api_calls=%d",
            metrics.recuperated_count,
            metrics.still_failed_count,
            metrics.api_calls_count,
        )
    finally:
        job.commit()


if __name__ == "__main__":
    main()
