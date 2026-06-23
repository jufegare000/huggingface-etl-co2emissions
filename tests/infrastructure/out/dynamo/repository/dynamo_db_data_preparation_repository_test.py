from typing import cast
from unittest.mock import MagicMock, call, patch
import pytest

from domain.data_preparation.models.preparation.final_manifest import FinalManifest
from domain.data_preparation.models.preparation.input_manifest import InputManifest
from domain.data_preparation.models.preparation.partition_descriptor import PartitionDescriptor
from domain.data_preparation.models.preparation.persistence_structure import PersistenceStructure
from infrastructure.out.dynamo.mappers.dynamo_db_data_preparation_mapper import DynamoDBDataPreparationMapper
from infrastructure.out.dynamo.repository.dynamo_db_data_preparation_repository import DynamoDBDataPreparationRepository
from infrastructure.out.dynamo.services.type_conversion_service import TypeConversionService

VAL_BUCKET = "test-bucket"
VAL_CONTROL_TABLE_NAME = "control-table-production"
VAL_MANIFEST_KEY = "manifests/run_123.json"
VAL_MANIFEST_PATH = "s3://test-bucket/manifests/run_123.json"

KEY_RUN = "PK_RUN"
VAL_RUN = "RUN_DATA"
KEY_LAST_RUN = "PK_LAST"
VAL_LAST_RUN = "LAST_RUN_DATA"
KEY_PARTITION = "PK_PART"
VAL_PART_1 = "PART_1_DATA"
VAL_PART_2 = "PART_2_DATA"

VAL_CONV_RUN = "CONVERTED_RUN_DATA"
VAL_CONV_LAST_RUN = "CONVERTED_LAST_RUN_DATA"
VAL_CONV_PART_1 = "CONVERTED_PART_1_DATA"
VAL_CONV_PART_2 = "CONVERTED_PART_2_DATA"

DICT_RUN = {KEY_RUN: VAL_RUN}
DICT_LAST_RUN = {KEY_LAST_RUN: VAL_LAST_RUN}
DICT_PART_1 = {KEY_PARTITION: VAL_PART_1}
DICT_PART_2 = {KEY_PARTITION: VAL_PART_2}

DICT_CONV_RUN = {KEY_RUN: VAL_CONV_RUN}
DICT_CONV_LAST_RUN = {KEY_LAST_RUN: VAL_CONV_LAST_RUN}
DICT_CONV_PART_1 = {KEY_PARTITION: VAL_CONV_PART_1}
DICT_CONV_PART_2 = {KEY_PARTITION: VAL_CONV_PART_2}

PATCH_DYNAMODB_CLIENT = "infrastructure.out.dynamo.dynamo_db_client.DynamoDBClient.dynamodb_client"


@pytest.fixture
def mock_dynamo_data_preparation_mapper() -> MagicMock:
    mock = MagicMock(spec=DynamoDBDataPreparationMapper)
    mock.to_run_item.return_value = DICT_RUN
    mock.to_last_run_item.return_value = DICT_LAST_RUN
    mock.to_partition_item.side_effect = [DICT_PART_1, DICT_PART_2]
    return mock


@pytest.fixture
def mock_type_conversion_service() -> MagicMock:
    mock = MagicMock(spec=TypeConversionService)
    mock.convert_floats_to_decimal.side_effect = [
        DICT_CONV_RUN,
        DICT_CONV_LAST_RUN,
        DICT_CONV_PART_1,
        DICT_CONV_PART_2,
    ]
    return mock


@pytest.fixture
def mock_input_manifest() -> MagicMock:
    mock = MagicMock(spec=InputManifest)
    mock.control_table_name = VAL_CONTROL_TABLE_NAME
    return mock


@pytest.fixture
def mock_final_manifest() -> MagicMock:
    mock = MagicMock(spec=FinalManifest)
    mock.manifest_path = VAL_MANIFEST_PATH
    mock.partitions = [MagicMock(spec=PartitionDescriptor), MagicMock(spec=PartitionDescriptor)]
    return mock


@pytest.fixture
def repository(
    mock_dynamo_data_preparation_mapper, mock_type_conversion_service
) -> DynamoDBDataPreparationRepository:
    return DynamoDBDataPreparationRepository(
        dynamo_data_preparation_mapper=cast(
            DynamoDBDataPreparationMapper, cast(object, mock_dynamo_data_preparation_mapper)
        ),
        type_conversion_service=cast(
            TypeConversionService, cast(object, mock_type_conversion_service)
        ),
    )


def test_persist_preparation_output(
    repository,
    mock_dynamo_data_preparation_mapper,
    mock_type_conversion_service,
    mock_input_manifest,
    mock_final_manifest,
):
    mock_client = MagicMock()
    mock_table = MagicMock()
    mock_batch = MagicMock()

    mock_client.Table.return_value = mock_table
    mock_table.batch_writer.return_value.__enter__.return_value = mock_batch

    with patch(PATCH_DYNAMODB_CLIENT, mock_client):
        result = repository.persist_preparation_output(
            VAL_BUCKET, mock_input_manifest, mock_final_manifest, VAL_MANIFEST_KEY
        )

    mock_client.Table.assert_called_once_with(VAL_CONTROL_TABLE_NAME)

    expected_table_calls = [call(Item=DICT_CONV_RUN), call(Item=DICT_CONV_LAST_RUN)]
    mock_table.put_item.assert_has_calls(expected_table_calls)

    expected_mapper_partition_calls = [
        call(mock_input_manifest, mock_final_manifest.partitions[0]),
        call(mock_input_manifest, mock_final_manifest.partitions[1]),
    ]
    mock_dynamo_data_preparation_mapper.to_partition_item.assert_has_calls(
        expected_mapper_partition_calls
    )

    expected_batch_calls = [call(Item=DICT_CONV_PART_1), call(Item=DICT_CONV_PART_2)]
    mock_batch.put_item.assert_has_calls(expected_batch_calls)

    assert isinstance(result, PersistenceStructure)
    assert result.manifest_path == VAL_MANIFEST_PATH
    assert result.partitions_count == len(mock_final_manifest.partitions)