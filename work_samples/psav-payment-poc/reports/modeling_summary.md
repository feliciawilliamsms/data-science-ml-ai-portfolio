# Modeling Summary Template

## Objective
Predict `paid_flag` for claims using non-leaky features available at prediction time.

## Baseline modeling workflow
1. Read staged parquet data.
2. Clean with `src/data_cleaning.py`.
3. Create engineered features with `src/feature_engineering.py`.
4. Split train/test using stratification on `paid_flag`.
5. Apply K-fold target encoding on selected high-cardinality columns using training data only.
6. Train baseline model(s).
7. Save metrics and model artifacts.

## Planned models
- Logistic Regression
- XGBoost
- LightGBM

## Initial feature groups
### Numeric / financial
- `savings`
- `totalcharges`
- `noncoveredcharges`
- `reasonableandcustomary`
- `days_since_received`
- `billed_amount_sum`
- engineered log / ratio features

### Categorical
- `cpt`
- `providergroupname`
- `client_code`
- `zz1_acctproduct_op1`
- `claim_type`
- `provider_state`
- other cleaned categorical columns

## Metrics to report
- Accuracy
- ROC AUC
- Precision / Recall / F1
- Confusion matrix
- Calibration review if probabilities are used operationally

## Artifact locations
- Models: `artifacts/models/`
- Figures: `artifacts/figures/`
- Reports: `reports/`

## Notes
- Confirm the deployed scoring feature set only includes features available at prediction time.
- Revisit any feature that may be operationally delayed or derived after adjudication.
