import logging
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config.injection.dependency_injector import build_error_redistribution_service

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def handler(event: dict, _context: Any) -> dict:
    run_id = event["run_id"]
    bucket = event["bucket_name"]
    control_table_name = event.get("control_table_name") or os.environ["CONTROL_TABLE_NAME"]

    logging.info("error_redistribution started: run_id=%s", run_id)

    service = build_error_redistribution_service(control_table_name)
    retry_partition_ids = service.run(run_id=run_id, bucket=bucket)

    retry_partitions = [{"partition_id": pid} for pid in retry_partition_ids]

    logging.info(
        "error_redistribution complete: run_id=%s, retry_partitions=%d",
        run_id,
        len(retry_partitions),
    )

    return {
        **event,
        "retry_partitions": retry_partitions,
        "has_retries": len(retry_partitions) > 0,
    }
