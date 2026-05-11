
import pandas as pd
import numpy as np
from pathlib import Path


def test_percentile_extraction():

    df = pd.read_csv(Path(__file__).resolve().parents[4] / "models.csv")

    k = 8
    percentiles = np.linspace(0, 1, k + 1)
    boundaries = df["co2_eq_emissions"].quantile(percentiles).to_list()

    for i in range(k):
        lo = boundaries[i]
        hi = boundaries[i + 1]
        print(f"Shard {i+1}: [{lo}, {hi})")



