from enum import Enum

class DataPreparationConfig(Enum):
    DISCOVERY_DEFAULT_KEY = "discovery/hf-carbon/latest/models_with_emissions.csv"
    PREPARED_PREFIX = "prepared/hf-carbon"
    GLOBAL_RATE_LIMIT = 1000
    WINDOW_SECONDS = 300
    CALLS_PER_MODEL = 1