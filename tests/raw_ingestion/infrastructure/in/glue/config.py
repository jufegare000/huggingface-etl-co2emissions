import sys
from dataclasses import dataclass


def get_glue_args(required_args):
    # noinspection PyUnresolvedReferences
    from awsglue.utils import getResolvedOptions  # type: ignore
    return getResolvedOptions(sys.argv, required_args)


@dataclass
class GlueConfig:
    job_name: str
    environment: str = "dev"
    bronze_path: str | None = None
    log_level: str = "INFO"


def load_glue_config() -> GlueConfig:
    args = get_glue_args(["job_name", "environment", "bronze_path", "log_level"])

    return GlueConfig(
        job_name=args["job_name"],
        environment=args["environment"],
        bronze_path=args["bronze_path"],
        log_level=args["log_level"],
    )