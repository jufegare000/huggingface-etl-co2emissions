from application.raw_ingestion import run_raw_ingestion
from config import load_glue_config

def main():
    config = load_glue_config()

    if config.job_name == "raw_ingestion":
        run_raw_ingestion(config)
    else:
        raise ValueError(f"Unsupported job_name: {config.job_name}")


if __name__ == "__main__":
    main()