import json
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True,
)

from awsglue.utils import getResolvedOptions
from data_discovery.application.services.discovery.discovery_service import run_discovery


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job_name = args["JOB_NAME"]

    logging.info(json.dumps({"event": "glue_job_started", "job_name": job_name}))

    result = run_discovery()

    logging.info(json.dumps({"event": "glue_job_finished", "job_name": job_name, "result": result}))


if __name__ == "__main__":
    main()