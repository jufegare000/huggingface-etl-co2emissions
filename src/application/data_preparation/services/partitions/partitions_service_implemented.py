from domain.data_preparation.models.preparation.input_manifest import InputManifest
from domain.data_preparation.models.preparation.persistence_structure import PersistenceStructure
from domain.data_preparation.models.preparation.step_functions_input import StepFunctionInput
from domain.data_preparation.persistence.data_preparation_repository import DataPreparationRepository
from domain.data_preparation.services.config.boundaries.boundaries_calculation_service import \
    BoundariesCalculationService
from domain.data_preparation.services.config.lambda_function.lambda_config_service import LambdaConfigService
from domain.data_preparation.services.config.manifests.data_manifest_builder_service import DataManifestBuilderService
from domain.data_preparation.services.config.partitions.partition_descriptor_service import PartitionDescriptorService
from domain.data_preparation.services.metadata.ai_metadata_models.models_metadata_service import AIModelsMetadataService
from domain.data_preparation.services.partitions.partitions_service import PartitionsService
from domain.data_preparation.models.preparation.partition_descriptor import PartitionDescriptor
from domain.data_preparation.services.s3.s3_service import S3Service


class PartitionsServiceImplemented(PartitionsService):

    def __init__(self, lambda_config_service: LambdaConfigService,
                 models_metadata_service: AIModelsMetadataService,
                 boundaries_calculation_service: BoundariesCalculationService,
                 partition_descriptor_service: PartitionDescriptorService,
                 data_manifest_builder: DataManifestBuilderService,
                 s3_service: S3Service,
                 data_preparation_repository: DataPreparationRepository,

                 ) -> None:
        self.lambda_config_service = lambda_config_service
        self.models_metadata_service = models_metadata_service
        self.boundaries_calculation_service = boundaries_calculation_service
        self.partition_descriptor_service = partition_descriptor_service
        self.data_manifest_builder = data_manifest_builder
        self.s3_service = s3_service
        self.data_preparation_repository = data_preparation_repository

    def build_partition_structure(
            self,
    ) -> StepFunctionInput:
        manifest: InputManifest = self.lambda_config_service.load_input_manifest()

        ai_models_metadata = self.models_metadata_service.load_models_metadata(manifest)

        boundaries = self.boundaries_calculation_service.calculate_percentile_boundaries(ai_models_metadata,
                                                                                         manifest.workers)

        partitions = self.partition_descriptor_service.build_partition_descriptors(
            boundaries,
            manifest,
            ai_models_metadata,
        )

        persistence_result: PersistenceStructure = self.persist_preparation_output(
            partitions,
            manifest.bucket_name,
            manifest,
        )

        step_function_input: StepFunctionInput = StepFunctionInput(
            partitions=partitions,
            persistence_result=persistence_result,
            bucket_name=manifest.bucket_name,
            input_manifest=manifest
        )

        return step_function_input

    def persist_preparation_output(
            self,
            partitions: list[PartitionDescriptor],
            bucket: str,
            manifest_input: InputManifest,
    ) -> PersistenceStructure:
        manifest, manifest_key = self.data_manifest_builder.build_manifest(partitions, bucket, manifest_input)

        self.s3_service.write_json_to_s3(manifest.to_dict(), bucket, manifest_key)

        persistence_structure = self.data_preparation_repository.persist_preparation_output(bucket, manifest_input,
                                                                                            manifest,
                                                                                            manifest_key)
        return persistence_structure
