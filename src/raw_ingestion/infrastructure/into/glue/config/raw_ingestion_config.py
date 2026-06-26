import sys
from dataclasses import dataclass


@dataclass
class RawIngestionJobConfig:
    job_name: str
    run_id: str
    partition_id: str
    control_table_name: str


def load_config() -> RawIngestionJobConfig:
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "run_id",
            "partition_id",
            "control_table_name",
        ],
    )

    partition_id = _normalize_partition_id(args["partition_id"])

    return RawIngestionJobConfig(
        job_name=args["JOB_NAME"],
        run_id=args["run_id"],
        partition_id=partition_id,
        control_table_name=args["control_table_name"],
    )


def _normalize_partition_id(value: str) -> str:
    text = str(value)
    return f"{int(text):06d}" if text.isdigit() else text
