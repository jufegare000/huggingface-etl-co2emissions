import os

TARGET_BUCKET = os.environ["CUSTOMER_TARGET_BUCKET_NAME"]
HF_TOKEN_SECRET_NAME = os.environ["CUSTOMER_HF_TOKEN_SECRET_NAME"]

BASE_PREFIX = "discovery/hf-carbon"
CHECKPOINT_KEY = f"{BASE_PREFIX}/checkpoints/latest.json"
HF_MODELS_URL = "https://huggingface.co/api/models"

PAGE_LIMIT = 1000
REQUEST_TIMEOUT_SECONDS = 60
FLUSH_EVERY_PAGES = 5
FLUSH_EVERY_MATCHES = 100
MAX_429_RETRIES_PER_RUN = 5
DEFAULT_429_SLEEP_SECONDS = 180

CSV_COLUMNS = [
    "model_id", "co2_eq_emissions", "co2_source", "training_type",
    "geographical_location", "hardware_used", "created_at", "downloads",
    "likes", "library_name", "pipeline_tag", "tags", "snapshot_id", "discovered_at",
]