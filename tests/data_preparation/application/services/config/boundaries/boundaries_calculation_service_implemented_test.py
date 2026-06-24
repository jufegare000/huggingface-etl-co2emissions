from unittest.mock import patch
import pytest

from data_preparation.application.services.config.boundaries.boundaries_calculation_service_implemented import \
    BoundariesCalculationServiceImplemented
from data_preparation.application.services.config.lambda_function.data_preparation_enum import (
    DataPreparationConfigParams,
)

CONFIG_GLOBAL_RATE_LIMIT = 4
CONFIG_CALLS_PER_MODEL = 1

WORKERS_TWO = 2
WORKERS_ONE_HUNDRED = 100

CO2_KEY = "co2_eq_emissions"

EMISSION_10 = 10.0
EMISSION_20 = 20.0
EMISSION_30 = 30.0

ID_0 = 0
ID_1 = 1

IDX_0 = 0
IDX_1 = 1
IDX_2 = 2
IDX_3 = 3

COUNT_0 = 0
COUNT_1 = 1
COUNT_2 = 2


@pytest.fixture
def service():
    return BoundariesCalculationServiceImplemented()


@pytest.fixture
def mock_config():
    with patch.object(
        DataPreparationConfigParams, "GLOBAL_RATE_LIMIT", CONFIG_GLOBAL_RATE_LIMIT
    ), patch.object(
        DataPreparationConfigParams, "CALLS_PER_MODEL", CONFIG_CALLS_PER_MODEL
    ):
        yield


def test_calculate_percentile_boundaries_standard(service, mock_config):
    models = [
        {CO2_KEY: EMISSION_10},
        {CO2_KEY: EMISSION_30},
        {CO2_KEY: EMISSION_20},
    ]

    result = service.calculate_percentile_boundaries(models, WORKERS_TWO)

    assert len(result) == COUNT_2

    assert result[IDX_0].partition_id == ID_0
    assert result[IDX_0].start_index == IDX_0
    assert result[IDX_0].end_index == IDX_2
    assert result[IDX_0].records_count == COUNT_2
    assert result[IDX_0].emission_min == EMISSION_10
    assert result[IDX_0].emission_max == EMISSION_30

    assert result[IDX_1].partition_id == ID_1
    assert result[IDX_1].start_index == IDX_2
    assert result[IDX_1].end_index == IDX_3
    assert result[IDX_1].records_count == COUNT_1
    assert result[IDX_1].emission_min == EMISSION_20
    assert result[IDX_1].emission_max == EMISSION_20


def test_calculate_percentile_boundaries_empty_models(service, mock_config):
    models = []

    result = service.calculate_percentile_boundaries(models, WORKERS_TWO)

    assert len(result) == COUNT_0


def test_calculate_percentile_boundaries_missing_and_none_emissions(
    service, mock_config
):
    models = [
        {},
        {CO2_KEY: None},
    ]

    result = service.calculate_percentile_boundaries(models, WORKERS_TWO)

    assert len(result) == COUNT_1
    assert result[IDX_0].partition_id == ID_0
    assert result[IDX_0].start_index == IDX_0
    assert result[IDX_0].end_index == IDX_2
    assert result[IDX_0].records_count == COUNT_2
    assert result[IDX_0].emission_min is None
    assert result[IDX_0].emission_max is None


def test_calculate_percentile_boundaries_floor_partition_size(
    service, mock_config
):
    models = [
        {CO2_KEY: EMISSION_10},
        {CO2_KEY: EMISSION_20},
    ]

    result = service.calculate_percentile_boundaries(models, WORKERS_ONE_HUNDRED)

    assert len(result) == COUNT_2

    assert result[IDX_0].partition_id == ID_0
    assert result[IDX_0].start_index == IDX_0
    assert result[IDX_0].end_index == IDX_1
    assert result[IDX_0].records_count == COUNT_1

    assert result[IDX_1].partition_id == ID_1
    assert result[IDX_1].start_index == IDX_1
    assert result[IDX_1].end_index == IDX_2
    assert result[IDX_1].records_count == COUNT_1