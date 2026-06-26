import sys
from dataclasses import dataclass


@dataclass
class DataRecuperationJobConfig:
    job_name: str
    run_id: str
    control_table_name: str
    source_bucket: str


def load_recuperation_config() -> DataRecuperationJobConfig:
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "run_id",
            "control_table_name",
            "source_bucket",
        ],
    )

    return DataRecuperationJobConfig(
        job_name=args["JOB_NAME"],
        run_id=args["run_id"],
        control_table_name=args["control_table_name"],
        source_bucket=args["source_bucket"],
    )
