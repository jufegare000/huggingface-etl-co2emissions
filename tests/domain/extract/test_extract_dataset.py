from types import SimpleNamespace

from domain.extract.extract_dataset import extract_datasets

def test_extract_datasets_from_carddata_list():
    info = SimpleNamespace(
        cardData={"datasets": ["squad", "cnn_dailymail", "squad"]},
        tags=[]
    )

    result = extract_datasets(info)

    assert result == "cnn_dailymail, squad"


def test_extract_datasets_from_carddata_string():
    info = SimpleNamespace(
        cardData={"datasets": "squad"},
        tags=[]
    )

    result = extract_datasets(info)

    assert result == "squad"


def test_extract_datasets_from_tags_when_carddata_missing():
    info = SimpleNamespace(
        cardData={},
        tags=["text-classification", "dataset:imdb", "dataset:ag_news", "dataset:imdb"]
    )

    result = extract_datasets(info)

    assert result == "ag_news, imdb"


def test_extract_datasets_returns_none_when_missing_everywhere():
    info = SimpleNamespace(
        cardData={},
        tags=["text-generation", "pytorch"]
    )

    result = extract_datasets(info)

    assert result is None