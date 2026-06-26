import pytest

from raw_ingestion.application.services.enrichment.row_enrichment_service import RowEnrichmentService

RUN_ID = "run-001"
PARTITION_ID = "000001"
NOW = "2024-01-01T12:00:00+00:00"

MINIMAL_ROW = {"model_id": "org/model-a"}
MINIMAL_HF_PAYLOAD = {"id": "org/model-a"}


@pytest.fixture
def service():
    return RowEnrichmentService()


# --- build_enriched_row ---

def test_build_enriched_row_sets_model_id(service):
    result = service.build_enriched_row(MINIMAL_ROW, MINIMAL_HF_PAYLOAD, RUN_ID, PARTITION_ID, NOW)
    assert result["model_id"] == "org/model-a"


def test_build_enriched_row_sets_run_id_and_partition_id(service):
    result = service.build_enriched_row(MINIMAL_ROW, MINIMAL_HF_PAYLOAD, RUN_ID, PARTITION_ID, NOW)
    assert result["run_id"] == RUN_ID
    assert result["partition_id"] == PARTITION_ID
    assert result["enriched_at"] == NOW


def test_build_enriched_row_extracts_downloads_and_likes(service):
    hf = {**MINIMAL_HF_PAYLOAD, "downloads": 1000, "likes": 50}
    result = service.build_enriched_row(MINIMAL_ROW, hf, RUN_ID, PARTITION_ID, NOW)
    assert result["downloads"] == 1000
    assert result["likes"] == 50


def test_build_enriched_row_co2_reported_false_when_missing(service):
    result = service.build_enriched_row(MINIMAL_ROW, MINIMAL_HF_PAYLOAD, RUN_ID, PARTITION_ID, NOW)
    assert result["co2_reported"] is False
    assert result["co2_eq_emissions"] is None


def test_build_enriched_row_co2_reported_true_when_present(service):
    row = {**MINIMAL_ROW, "co2_eq_emissions": "1.5"}
    result = service.build_enriched_row(row, MINIMAL_HF_PAYLOAD, RUN_ID, PARTITION_ID, NOW)
    assert result["co2_reported"] is True
    assert result["co2_eq_emissions"] == 1.5


def test_build_enriched_row_datasets_from_card_data(service):
    hf = {**MINIMAL_HF_PAYLOAD, "cardData": {"datasets": ["ds-a", "ds-b"]}}
    result = service.build_enriched_row(MINIMAL_ROW, hf, RUN_ID, PARTITION_ID, NOW)
    assert result["datasets_size"] == 2


def test_build_enriched_row_size_from_safetensors(service):
    hf = {**MINIMAL_HF_PAYLOAD, "safetensors": {"total": 7_000_000_000}}
    result = service.build_enriched_row(MINIMAL_ROW, hf, RUN_ID, PARTITION_ID, NOW)
    assert result["size"] == 7_000_000_000


def test_build_enriched_row_size_from_siblings(service):
    hf = {
        **MINIMAL_HF_PAYLOAD,
        "siblings": [{"size": 100}, {"size": 200}],
    }
    result = service.build_enriched_row(MINIMAL_ROW, hf, RUN_ID, PARTITION_ID, NOW)
    assert result["size"] == 300


def test_build_enriched_row_size_none_when_no_sibling_sizes(service):
    hf = {**MINIMAL_HF_PAYLOAD, "siblings": [{"rfilename": "model.bin"}]}
    result = service.build_enriched_row(MINIMAL_ROW, hf, RUN_ID, PARTITION_ID, NOW)
    assert result["size"] is None


def test_build_enriched_row_size_efficency_computed(service):
    hf = {**MINIMAL_HF_PAYLOAD, "downloads": 1000, "safetensors": {"total": 500}}
    result = service.build_enriched_row(MINIMAL_ROW, hf, RUN_ID, PARTITION_ID, NOW)
    assert result["size_efficency"] == 2.0


def test_build_enriched_row_domain_from_pipeline_tag(service):
    row = {**MINIMAL_ROW, "pipeline_tag": "text-classification"}
    result = service.build_enriched_row(row, MINIMAL_HF_PAYLOAD, RUN_ID, PARTITION_ID, NOW)
    assert result["domain"] == "text-classification"


def test_build_enriched_row_domain_from_hf_tags(service):
    hf = {**MINIMAL_HF_PAYLOAD, "tags": ["nlp", "text"]}
    result = service.build_enriched_row(MINIMAL_ROW, hf, RUN_ID, PARTITION_ID, NOW)
    assert result["domain"] == "nlp"


def test_build_enriched_row_auto_true_when_autotrain_tag(service):
    hf = {**MINIMAL_HF_PAYLOAD, "tags": ["autotrain", "text-classification"]}
    result = service.build_enriched_row(MINIMAL_ROW, hf, RUN_ID, PARTITION_ID, NOW)
    assert result["auto"] is True


def test_build_enriched_row_auto_false_when_no_auto_tag(service):
    hf = {**MINIMAL_HF_PAYLOAD, "tags": ["text-classification"]}
    result = service.build_enriched_row(MINIMAL_ROW, hf, RUN_ID, PARTITION_ID, NOW)
    assert result["auto"] is False


def test_build_enriched_row_performance_score_max_of_metrics(service):
    hf = {**MINIMAL_HF_PAYLOAD, "cardData": {"metrics": [{"value": 0.8}, {"value": 0.95}]}}
    result = service.build_enriched_row(MINIMAL_ROW, hf, RUN_ID, PARTITION_ID, NOW)
    assert result["performance_score"] == 0.95


# --- build_error_row ---

def test_build_error_row_captures_exception_type(service):
    exc = ValueError("bad input")
    result = service.build_error_row(MINIMAL_ROW, exc, RUN_ID, PARTITION_ID, NOW)
    assert result["error_type"] == "ValueError"
    assert result["error_message"] == "bad input"


def test_build_error_row_sets_model_id(service):
    exc = RuntimeError("fail")
    result = service.build_error_row(MINIMAL_ROW, exc, RUN_ID, PARTITION_ID, NOW)
    assert result["model_id"] == "org/model-a"


def test_build_error_row_sets_failed_at(service):
    exc = Exception("oops")
    result = service.build_error_row(MINIMAL_ROW, exc, RUN_ID, PARTITION_ID, NOW)
    assert result["failed_at"] == NOW


# --- _safe_int ---

def test_safe_int_returns_zero_for_none(service):
    assert service._safe_int(None) == 0


def test_safe_int_parses_string(service):
    assert service._safe_int("42") == 42


def test_safe_int_truncates_float(service):
    assert service._safe_int("3.9") == 3


def test_safe_int_returns_zero_on_invalid(service):
    assert service._safe_int("not-a-number") == 0


# --- _safe_float ---

def test_safe_float_returns_none_for_none(service):
    assert service._safe_float(None) is None


def test_safe_float_parses_string(service):
    assert service._safe_float("1.5") == 1.5


def test_safe_float_returns_none_on_invalid(service):
    assert service._safe_float("abc") is None


# --- _safe_json ---

def test_safe_json_returns_empty_string_for_none(service):
    assert service._safe_json(None) == ""


def test_safe_json_serializes_list(service):
    import json
    result = service._safe_json(["a", "b"])
    assert json.loads(result) == ["a", "b"]


# --- _extract_card_data ---

def test_extract_card_data_returns_empty_if_missing(service):
    assert service._extract_card_data({}) == {}


def test_extract_card_data_falls_back_to_card_data_key(service):
    result = service._extract_card_data({"card_data": {"env": "cpu"}})
    assert result == {"env": "cpu"}


def test_extract_card_data_returns_empty_if_not_dict(service):
    assert service._extract_card_data({"cardData": "invalid"}) == {}


# --- _extract_datasets ---

def test_extract_datasets_from_card_data(service):
    card = {"datasets": ["ds1", "ds2"]}
    result = service._extract_datasets(card, {})
    assert result == ["ds1", "ds2"]


def test_extract_datasets_wraps_string_in_list(service):
    card = {"datasets": "single-ds"}
    result = service._extract_datasets(card, {})
    assert result == ["single-ds"]


def test_extract_datasets_falls_back_to_hf_payload(service):
    result = service._extract_datasets({}, {"datasets": ["from-hf"]})
    assert result == ["from-hf"]


def test_extract_datasets_returns_empty_when_absent(service):
    assert service._extract_datasets({}, {}) == []


# --- _extract_performance_score ---

def test_extract_performance_score_returns_none_when_no_metrics(service):
    assert service._extract_performance_score(None) is None


def test_extract_performance_score_picks_max_from_nested_dict(service):
    metrics = {"task1": {"value": 0.7}, "task2": {"value": 0.9}}
    assert service._extract_performance_score(metrics) == 0.9


def test_extract_performance_score_picks_max_from_flat_list(service):
    assert service._extract_performance_score([0.5, 0.8, 0.6]) == 0.8
