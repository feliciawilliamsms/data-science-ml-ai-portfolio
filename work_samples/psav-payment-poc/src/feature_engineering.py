from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.model_selection import StratifiedKFold


def create_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create non-leaky engineered features identified during EDA.
    """
    df = df.copy()

    if "billed_amount_sum" in df.columns:
        df["log_billed_amount_sum"] = np.log1p(df["billed_amount_sum"].clip(lower=0))

    if "reasonableandcustomary" in df.columns:
        df["log_reasonableandcustomary"] = np.log1p(
            df["reasonableandcustomary"].clip(lower=0)
        )

    if "noncoveredcharges" in df.columns:
        df["has_noncoveredcharges"] = (df["noncoveredcharges"] > 0).astype(int)
        df["log_noncoveredcharges"] = np.log1p(df["noncoveredcharges"].clip(lower=0))

    if {"reasonableandcustomary", "billed_amount_sum"}.issubset(df.columns):
        denom = df["billed_amount_sum"].clip(lower=0) + 1.0
        df["rnc_to_billed_ratio"] = df["reasonableandcustomary"].clip(lower=0) / denom

    if {"noncoveredcharges", "billed_amount_sum"}.issubset(df.columns):
        denom = df["billed_amount_sum"].clip(lower=0) + 1.0
        df["noncovered_to_billed_ratio"] = df["noncoveredcharges"].clip(lower=0) / denom

    if "days_since_received" in df.columns:
        df["claim_age_bucket"] = pd.cut(
            df["days_since_received"],
            bins=[-np.inf, 30, 90, 180, 365, np.inf],
            labels=["0-30", "31-90", "91-180", "181-365", "365+"],
        ).astype("string")

    return df


def fit_target_encoding_map(
    df: pd.DataFrame,
    column: str,
    target: str,
    smoothing: int = 20,
) -> tuple[dict, float]:
    global_mean = df[target].mean()

    stats = df.groupby(column, dropna=False)[target].agg(["mean", "count"])
    smoothing_factor = 1 / (1 + np.exp(-(stats["count"] - smoothing)))

    stats["encoded"] = (
        global_mean * (1 - smoothing_factor) + stats["mean"] * smoothing_factor
    )

    return stats["encoded"].to_dict(), float(global_mean)


def transform_target_encoding(
    df: pd.DataFrame,
    column: str,
    mapping: dict,
    global_mean: float,
) -> pd.Series:
    return df[column].map(mapping).fillna(global_mean)


def kfold_target_encode(
    df: pd.DataFrame,
    column: str,
    target: str,
    n_splits: int = 5,
    smoothing: int = 20,
    random_state: int = 42,
) -> tuple[pd.Series, dict, float]:
    """
    Out-of-fold target encoding for a training set.
    """
    df = df.copy()
    skf = StratifiedKFold(
        n_splits=n_splits,
        shuffle=True,
        random_state=random_state,
    )

    encoded = pd.Series(index=df.index, dtype=float)

    for train_idx, valid_idx in skf.split(df, df[target]):
        train_fold = df.iloc[train_idx]
        valid_fold = df.iloc[valid_idx]

        mapping, global_mean = fit_target_encoding_map(
            train_fold,
            column=column,
            target=target,
            smoothing=smoothing,
        )

        encoded.iloc[valid_idx] = transform_target_encoding(
            valid_fold,
            column=column,
            mapping=mapping,
            global_mean=global_mean,
        )

    full_mapping, full_global_mean = fit_target_encoding_map(
        df,
        column=column,
        target=target,
        smoothing=smoothing,
    )

    return encoded, full_mapping, full_global_mean
