from infrastructure.out.hugging_face.hf_client import build_hf_api
from infrastructure.out.secrets_config.hf_token_config import HFTokenClass

def test_call_hf_api():
    token = HFTokenClass().get_hf_token_from_call
    hf_api = build_hf_api(token=token)

    models = list(hf_api.list_models(limit=1))

    assert hf_api is not None
    assert len(models) == 1