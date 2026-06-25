import csv
import io
from unittest.mock import MagicMock

import pytest

from data_discovery.application.services.consolidation.snapshot_consolidation_service_implemented import (
    SnapshotConsolidationServiceImplemented,
)
from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from shared.domain.models.datasets.bronze_datasets_columns import CSV_BROZE_COLUMNS

BUCKET = "my-discovery-bucket"
SNAPSHOT_ID = "20240101T120000"
PARTS_PREFIX = f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={SNAPSHOT_ID}/filtered_parts/"
SNAPSHOT_KEY = f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={SNAPSHOT_ID}/filtered/models_with_emissions.csv"
LATEST_KEY = f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/latest/models_with_emissions.csv"


def _make_csv(rows):
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_BROZE_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue()


def _row(model_id, co2="1.0"):
    return {col: "" for col in CSV_BROZE_COLUMNS} | {"model_id": model_id, "co2_eq_emissions": co2}


@pytest.fixture
def mock_reader():
    return MagicMock()


@pytest.fixture
def mock_writer():
    return MagicMock()


@pytest.fixture
def mock_uploader():
    m = MagicMock()
    m.rows_to_csv_text.side_effect = lambda rows: _make_csv(rows)
    return m


@pytest.fixture
def service(mock_reader, mock_writer, mock_uploader):
    return SnapshotConsolidationServiceImplemented(
        s3_reader_service=mock_reader,
        s3_writer_service=mock_writer,
        uploader_process_service=mock_uploader,
        target_bucket=BUCKET,
    )


# --- consolidate_parts: basic happy path ---

def test_consolidate_parts_returns_snapshot_uri_and_count(service, mock_reader):
    part_key = f"{PARTS_PREFIX}models_with_emissions_part_000001.csv"
    mock_reader.list_s3_keys.return_value = [part_key]
    mock_reader.read_s3_text.return_value = _make_csv([_row("org/model-a")])

    uri, count = service.consolidate_parts(SNAPSHOT_ID)

    assert uri == f"s3://{BUCKET}/{SNAPSHOT_KEY}"
    assert count == 1


def test_consolidate_parts_writes_snapshot_and_latest(service, mock_reader, mock_writer):
    part_key = f"{PARTS_PREFIX}models_with_emissions_part_000001.csv"
    mock_reader.list_s3_keys.return_value = [part_key]
    mock_reader.read_s3_text.return_value = _make_csv([_row("org/model-a")])

    service.consolidate_parts(SNAPSHOT_ID)

    written_keys = [c.args[2] for c in mock_writer.upload_text_to_s3.call_args_list]
    assert SNAPSHOT_KEY in written_keys
    assert LATEST_KEY in written_keys


# --- consolidate_parts: deduplication ---

def test_consolidate_parts_deduplicates_by_model_id(service, mock_reader):
    part1_key = f"{PARTS_PREFIX}models_with_emissions_part_000001.csv"
    part2_key = f"{PARTS_PREFIX}models_with_emissions_part_000002.csv"
    mock_reader.list_s3_keys.return_value = [part1_key, part2_key]

    row_v1 = _row("org/model-a", co2="1.0")
    row_v2 = _row("org/model-a", co2="2.0")
    mock_reader.read_s3_text.side_effect = [_make_csv([row_v1]), _make_csv([row_v2])]

    _, count = service.consolidate_parts(SNAPSHOT_ID)
    assert count == 1


def test_consolidate_parts_last_part_wins_on_duplicate(service, mock_reader, mock_uploader):
    part1_key = f"{PARTS_PREFIX}models_with_emissions_part_000001.csv"
    part2_key = f"{PARTS_PREFIX}models_with_emissions_part_000002.csv"
    mock_reader.list_s3_keys.return_value = [part1_key, part2_key]

    row_v1 = _row("org/model-a", co2="1.0")
    row_v2 = _row("org/model-a", co2="2.0")
    mock_reader.read_s3_text.side_effect = [_make_csv([row_v1]), _make_csv([row_v2])]

    service.consolidate_parts(SNAPSHOT_ID)

    rows_passed = mock_uploader.rows_to_csv_text.call_args.args[0]
    assert rows_passed[0]["co2_eq_emissions"] == "2.0"


# --- consolidate_parts: filters non-csv keys ---

def test_consolidate_parts_ignores_non_csv_keys(service, mock_reader):
    mock_reader.list_s3_keys.return_value = [
        f"{PARTS_PREFIX}models_with_emissions_part_000001.csv",
        f"{PARTS_PREFIX}_metadata/something.json",
    ]
    mock_reader.read_s3_text.return_value = _make_csv([_row("org/model-a")])

    _, count = service.consolidate_parts(SNAPSHOT_ID)

    assert mock_reader.read_s3_text.call_count == 1
    assert count == 1


# --- consolidate_parts: empty snapshot ---

def test_consolidate_parts_empty_returns_zero_rows(service, mock_reader):
    mock_reader.list_s3_keys.return_value = []

    uri, count = service.consolidate_parts(SNAPSHOT_ID)

    assert count == 0
    assert uri == f"s3://{BUCKET}/{SNAPSHOT_KEY}"


# --- consolidate_parts: skips rows without model_id ---

def test_consolidate_parts_skips_rows_missing_model_id(service, mock_reader):
    part_key = f"{PARTS_PREFIX}models_with_emissions_part_000001.csv"
    mock_reader.list_s3_keys.return_value = [part_key]

    bad_row = {col: "" for col in CSV_BROZE_COLUMNS}
    bad_row["model_id"] = ""
    mock_reader.read_s3_text.return_value = _make_csv([bad_row])

    _, count = service.consolidate_parts(SNAPSHOT_ID)
    assert count == 0
