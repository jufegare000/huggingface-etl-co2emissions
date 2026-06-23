from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from typing import Any
from domain.data_preparation.models.preparation.final_manifest import FinalManifest
from domain.data_preparation.models.preparation.input_manifest import InputManifest
from domain.data_preparation.models.preparation.manifest_status import ManifestStatus
from domain.data_preparation.models.preparation.partition_descriptor import PartitionDescriptor
from domain.data_preparation.models.preparation.persistence_structure import PersistenceStructure
from domain.data_preparation.models.preparation.step_function_output import StepFunctionOutput
from config.injection.dependency_injector import data_parsing_service
from config.injection.dependency_injector import s3_service
from config.injection.dependency_injector import lambda_config_service
from config.injection.dependency_injector import models_metadata_service
from config.injection.dependency_injector import boundaries_calculation_service
from config.injection.dependency_injector import partition_descriptor_service
from config.injection.dependency_injector import data_preparation_repository

sys.path.insert(0, str(Path(__file__).resolve().parent))

def build_manifest(
        partitions: list[PartitionDescriptor],
        bucket: str,
        config: InputManifest,
) -> tuple[FinalManifest, str]:
    manifest_key = f"{config.prepared_prefix.strip('/')}/manifest.json"
    manifest = FinalManifest(
        run_id=config.run_id,
        status=ManifestStatus.PREPARED,
        source_csv_path=config.source_csv_path,
        manifest_path=f"s3://{bucket}/{manifest_key}",
        partitions_count=len(partitions),
        workers=config.workers,
        threads_per_worker=config.threads_per_worker,
        global_rate_limit=config.global_rate_limit,
        window_seconds=config.window_seconds,
        calls_per_model=config.calls_per_model,
        created_at=data_parsing_service.utc_now_iso(),
        partitions=partitions,
    )
    return manifest, manifest_key


def persist_preparation_output(
        partitions: list[PartitionDescriptor],
        bucket: str,
        config: InputManifest,
) -> PersistenceStructure:
    manifest, manifest_key = build_manifest(partitions, bucket, config)

    s3_service.write_json_to_s3(manifest.to_dict(), bucket, manifest_key)

    persistence_structure = data_preparation_repository.persist_preparation_output(bucket, config, manifest,
                                                                                   manifest_key)
    return persistence_structure


def build_step_function_output(
        partitions: list[PartitionDescriptor],
        persistence_result: PersistenceStructure,
        bucket_name: str,
        config: InputManifest,
) -> StepFunctionOutput:
    return StepFunctionOutput(
        run_id=config.run_id,
        bucket_name=bucket_name,
        control_table_name=config.control_table_name,
        source_csv_path=config.source_csv_path,
        manifest_path=persistence_result.manifest_path,
        partitions_count=persistence_result.partitions_count,
        partitions=partitions,
    )


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    manifest: InputManifest = lambda_config_service.load_input_manifest(event)

    ai_models_metadata = models_metadata_service.load_models_metadata(manifest)

    boundaries = boundaries_calculation_service.calculate_percentile_boundaries(ai_models_metadata, manifest.workers)

    partitions = partition_descriptor_service.build_partition_descriptors(
        boundaries,
        manifest,
        ai_models_metadata,
    )

    persistence_result = persist_preparation_output(
        partitions,
        manifest.bucket_name,
        manifest,
    )

    return build_step_function_output(
        partitions,
        persistence_result,
        manifest.bucket_name,
        manifest,
    ).to_dict()
