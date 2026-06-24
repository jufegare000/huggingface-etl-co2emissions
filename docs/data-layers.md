# Data Layers

The pipeline uses a medallion architecture with four layers persisted in Amazon S3.

## Bronze

Raw snapshot of Hugging Face models reporting CO2 emissions.

| Field | Description |
|-------|-------------|
| `model_id` | Unique model identifier |
| `co2_eq_emissions` | Reported CO2 equivalent emissions |
| `downloads` | Download count |
| `likes` | Like count |
| `pipeline_tag` | Task category (e.g. text-classification) |
| `library_name` | ML framework (e.g. transformers) |
| `datasets` | Referenced dataset IDs |
| `created_at` | Model creation timestamp |

## Silver Models

Model-level enrichment added on top of the Bronze snapshot.

| Field | Description |
|-------|-------------|
| `model_id` | Unique model identifier |
| `model_size_mb` | Estimated model size in MB |
| `is_autotrain` | Whether the model was auto-trained |
| `training_type` | Training category (fine-tune, pre-train, etc.) |
| `geographical_location` | Reported training location |
| `hardware_used` | Hardware used during training |
| `performance_metrics` | Evaluation metrics extracted from model card |

## Silver Datasets

Dataset-level enrichment for datasets referenced by Bronze models.

| Field | Description |
|-------|-------------|
| `dataset_id` | Unique dataset identifier |
| `dataset_size` | Dataset size |
| *(optional)* | Additional dataset metadata |

## Gold

Final curated analytical dataset joining Silver Models and Silver Datasets.

| Field | Description |
|-------|-------------|
| `model_id` | Unique model identifier |
| `datasets` | Referenced dataset IDs |
| `datasets_size` | Resolved dataset sizes |
| `co2_eq_emissions` | CO2 equivalent emissions |
| `source` | Emission source field |
| `training_type` | Training category |
| `geographical_location` | Training location |
| `environment` | Deployment or training environment |
| `performance_metrics` | Evaluation metrics |
| `downloads` | Download count |
| `likes` | Like count |
| `library_name` | ML framework |
| `domain` | Model domain/task |
| `size` | Model size |
| `created_at` | Model creation timestamp |
| `auto` | Auto-train flag |
