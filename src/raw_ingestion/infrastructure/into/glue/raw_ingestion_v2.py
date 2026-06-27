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

from raw_ingestion.infrastructure.into.glue.config.injection.dependency_injector import (
    build_raw_ingestion_service,
)
from raw_ingestion.infrastructure.into.glue.config.raw_ingestion_config import load_config


def main() -> None:
    config = load_config()

    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(config.job_name, {})

    logging.info(
        "raw_ingestion job started: run_id=%s, partition_id=%s",
        config.run_id,
        config.partition_id,
    )

    try:
        service = build_raw_ingestion_service(
            table_name=config.control_table_name,
        )
        metrics = service.run(
            run_id=config.run_id,
            partition_id=config.partition_id,
            job_name=config.job_name,
        )
        logging.info(
            "raw_ingestion job completed: success=%d, failed=%d, api_calls=%d",
            metrics.success_count,
            metrics.failed_count,
            metrics.api_calls_count,
        )
    finally:
        job.commit()


if __name__ == "__main__":
    main()
