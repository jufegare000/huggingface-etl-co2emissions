import csv
import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError
from huggingface_hub import HfApi


# =============================================================================
# Configuración fija / entorno
# =============================================================================

TARGET_BUCKET = os.environ["CUSTOMER_TARGET_BUCKET_NAME"]
HF_TOKEN_SECRET_NAME = os.environ["CUSTOMER_HF_TOKEN_SECRET_NAME"]

BASE_PREFIX = "discovery/hf-carbon"
LATEST_KEY = f"{BASE_PREFIX}/latest/models_with_emissions.csv"

CSV_COLUMNS = [
    "model_id",
    "co2_eq_emissions",
    "co2_source",
    "training_type",
    "geographical_location",
    "hardware_used",
    "created_at",
    "downloads",
    "likes",
    "library_name",
    "pipeline_tag",
    "tags",
    "snapshot_id",
    "discovered_at",
]


# =============================================================================
# Secrets Manager
# =============================================================================

def get_hf_token_from_secrets_manager(secret_name: str) -> str:
    secrets_client = boto3.client("secretsmanager")

    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
    except ClientError as exc:
        raise RuntimeError(
            f"Could not read Hugging Face token from Secrets Manager secret: {secret_name}"
        ) from exc

    secret_string = response.get("SecretString")

    if not secret_string:
        raise ValueError(
            f"Secret {secret_name} does not contain SecretString. Binary secrets are not supported."
        )

    # Caso 1: el secreto es directamente el token plano.
    if not secret_string.strip().startswith("{"):
        token = secret_string.strip()
        if not token:
            raise ValueError(f"Secret {secret_name} is empty.")
        return token

    # Caso 2: el secreto es JSON.
    try:
        secret_json = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Secret {secret_name} is not valid JSON and could not be parsed as token."
        ) from exc

    for key in ("HF_TOKEN", "hf_token", "token"):
        token = secret_json.get(key)
        if token:
            return str(token).strip()

    raise ValueError(
        f"Secret {secret_name} is JSON but does not contain one of: HF_TOKEN, hf_token, token"
    )


# =============================================================================
# Helpers
# =============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_compact() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ")


def to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None

    if hasattr(value, "isoformat"):
        return value.isoformat()

    return str(value)


def safe_json(value: Any) -> str:
    if value is None:
        return ""

    try:
        return json.dumps(value, ensure_ascii=False)
    except TypeError:
        return json.dumps(str(value), ensure_ascii=False)


def card_data_to_dict(card_data: Any) -> Dict[str, Any]:
    if card_data is None:
        return {}

    if isinstance(card_data, dict):
        return card_data

    if hasattr(card_data, "to_dict"):
        return card_data.to_dict()

    if hasattr(card_data, "__dict__"):
        return {
            key: value
            for key, value in vars(card_data).items()
            if not key.startswith("_")
        }

    return {}


def normalize_co2_emissions(value: Any) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        cleaned = value.strip().lower()
        cleaned = cleaned.replace("kg", "")
        cleaned = cleaned.replace("g", "")
        cleaned = cleaned.replace("co2eq", "")
        cleaned = cleaned.replace("co2e", "")
        cleaned = cleaned.replace(",", "")
        cleaned = cleaned.strip()

        try:
            return float(cleaned)
        except ValueError:
            return None

    return None


def extract_co2_fields(card_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    co2 = card_data.get("co2_eq_emissions")

    if not co2:
        return None

    if not isinstance(co2, dict):
        return None

    emissions = normalize_co2_emissions(co2.get("emissions"))

    if emissions is None:
        return None

    return {
        "co2_eq_emissions": emissions,
        "co2_source": co2.get("source"),
        "training_type": co2.get("training_type"),
        "geographical_location": co2.get("geographical_location"),
        "hardware_used": co2.get("hardware_used"),
    }


def model_to_row(model: Any, snapshot_id: str, discovered_at: str) -> Optional[Dict[str, Any]]:
    card_data = card_data_to_dict(getattr(model, "card_data", None))
    co2_fields = extract_co2_fields(card_data)

    if co2_fields is None:
        return None

    return {
        "model_id": getattr(model, "id", None),
        "co2_eq_emissions": co2_fields["co2_eq_emissions"],
        "co2_source": co2_fields["co2_source"],
        "training_type": co2_fields["training_type"],
        "geographical_location": co2_fields["geographical_location"],
        "hardware_used": co2_fields["hardware_used"],
        "created_at": to_iso(getattr(model, "created_at", None)),
        "downloads": getattr(model, "downloads", None),
        "likes": getattr(model, "likes", None),
        "library_name": getattr(model, "library_name", None),
        "pipeline_tag": getattr(model, "pipeline_tag", None),
        "tags": safe_json(getattr(model, "tags", None)),
        "snapshot_id": snapshot_id,
        "discovered_at": discovered_at,
    }


def upload_file_to_s3(local_path: str, bucket: str, key: str) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)


# =============================================================================
# Main
# =============================================================================

def run_discovery() -> Dict[str, Any]:
    hf_token = get_hf_token_from_secrets_manager(HF_TOKEN_SECRET_NAME)

    snapshot_id = utc_now_compact()
    discovered_at = utc_now().isoformat()

    snapshot_key = (
        f"{BASE_PREFIX}/snapshot_id={snapshot_id}/filtered/models_with_emissions.csv"
    )

    total_models_seen = 0
    models_with_emissions = 0

    api = HfApi(token=hf_token)

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".csv",
        newline="",
        encoding="utf-8",
        delete=False,
    ) as tmp:
        writer = csv.DictWriter(tmp, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        models_iterator = api.list_models(
            cardData=True,
            full=True,
        )

        for model in models_iterator:
            total_models_seen += 1

            row = model_to_row(
                model=model,
                snapshot_id=snapshot_id,
                discovered_at=discovered_at,
            )

            if row is None:
                continue

            writer.writerow(row)
            models_with_emissions += 1

            if total_models_seen % 1000 == 0:
                print(
                    json.dumps(
                        {
                            "event": "discovery_progress",
                            "total_models_seen": total_models_seen,
                            "models_with_emissions": models_with_emissions,
                        }
                    )
                )

        local_csv_path = tmp.name

    upload_file_to_s3(
        local_path=local_csv_path,
        bucket=TARGET_BUCKET,
        key=snapshot_key,
    )

    upload_file_to_s3(
        local_path=local_csv_path,
        bucket=TARGET_BUCKET,
        key=LATEST_KEY,
    )

    result = {
        "snapshot_id": snapshot_id,
        "snapshot_path": f"s3://{TARGET_BUCKET}/{snapshot_key}",
        "latest_path": f"s3://{TARGET_BUCKET}/{LATEST_KEY}",
        "total_models_seen": total_models_seen,
        "models_with_emissions": models_with_emissions,
        "completed_at": utc_now().isoformat(),
    }

    print(json.dumps(result, ensure_ascii=False))
    return result


if __name__ == "__main__":
    run_discovery()