from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBClassifier

from data_cleaning import clean_dataset
from feature_engineering import create_features, kfold_target_encode, transform_target_encoding


TARGET = "paid_flag"

COLUMNS_TO_TARGET_ENCODE = [
    "cpt",
    "providergroupname",
    "client_code",
    "zz1_acctproduct_op1",
    "claim_type",
    "provider_state",
]


def build_model_pipeline(X: pd.DataFrame) -> Pipeline:
    numeric_cols = X.select_dtypes(include=["number", "bool"]).columns.tolist()
    categorical_cols = X.select_dtypes(include=["object", "string", "category"]).columns.tolist()

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                    ]
                ),
                numeric_cols,
            ),
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                    ]
                ),
                categorical_cols,
            ),
        ],
        remainder="drop",
    )

    model = XGBClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss",
        random_state=42,
        n_jobs=-1,
    )

    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )


def prepare_model_data(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_dataset(df)
    df = create_features(df)
    return df


def add_target_encoded_features(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    target: str = TARGET,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    train_df = train_df.copy()
    test_df = test_df.copy()
    encoding_maps = {}

    for col in COLUMNS_TO_TARGET_ENCODE:
        if col not in train_df.columns:
            continue

        train_encoded, mapping, global_mean = kfold_target_encode(
            df=train_df,
            column=col,
            target=target,
            n_splits=5,
            smoothing=20,
            random_state=42,
        )

        train_df[f"{col}_te"] = train_encoded
        test_df[f"{col}_te"] = transform_target_encoding(
            test_df,
            column=col,
            mapping=mapping,
            global_mean=global_mean,
        )

        encoding_maps[col] = {
            "mapping": mapping,
            "global_mean": global_mean,
        }

    return train_df, test_df, encoding_maps


def main() -> None:
    project_root = Path(__file__).resolve().parent.parent
    data_path = (
        project_root
        / "data"
        / "staging"
        / "psav_model_2025-01-01_to_2026-01-01_pulled_2026-03-06.parquet"
    )
    artifacts_dir = project_root / "artifacts"
    models_dir = artifacts_dir / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(data_path, engine="pyarrow")
    data = prepare_model_data(df)

    train_df, test_df = train_test_split(
        data,
        test_size=0.2,
        random_state=42,
        stratify=data[TARGET],
    )

    train_df, test_df, encoding_maps = add_target_encoded_features(train_df, test_df)

    X_train = train_df.drop(columns=[TARGET])
    y_train = train_df[TARGET]
    X_test = test_df.drop(columns=[TARGET])
    y_test = test_df[TARGET]

    pipeline = build_model_pipeline(X_train)
    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)
    y_prob = pipeline.predict_proba(X_test)[:, 1]

    metrics = {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "roc_auc": float(roc_auc_score(y_test, y_prob)),
        "classification_report": classification_report(y_test, y_pred, output_dict=True),
    }

    joblib.dump(pipeline, models_dir / "xgb_paid_flag_pipeline.joblib")
    joblib.dump(encoding_maps, models_dir / "target_encoding_maps.joblib")

    with open(models_dir / "metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    print("Saved model to:", models_dir / "xgb_paid_flag_pipeline.joblib")
    print("Saved target encoding maps to:", models_dir / "target_encoding_maps.joblib")
    print("Saved metrics to:", models_dir / "metrics.json")
    print("Accuracy:", metrics["accuracy"])
    print("ROC AUC:", metrics["roc_auc"])


if __name__ == "__main__":
    main()
