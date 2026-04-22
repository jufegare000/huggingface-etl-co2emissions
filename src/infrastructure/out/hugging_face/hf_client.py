from huggingface_hub import HfApi

def build_hf_api(token: str) -> HfApi:
    return HfApi(token=token)