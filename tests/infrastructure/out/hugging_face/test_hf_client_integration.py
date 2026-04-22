from infrastructure.out.hugging_face.hf_client import build_hf_api
from infrastructure.out.secrets_config.hf_token_config import HFTokenClass

from itertools import islice

def test_inspect_co2_ranges():
    token = HFTokenClass().get_hf_token_from_call
    hf_api = build_hf_api(token=token)

    models = list(
        islice(
            hf_api.list_models(
                emissions_thresholds=(0, None),
                cardData=True,
            ),
            10,
        )
    )

    for model in models:
        print(model.id)
        print(model.cardData)
        print("-----")

    assert models