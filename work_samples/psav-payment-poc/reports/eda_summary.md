# Exploratory Data Analysis Summary

## Project
PSAV Claim Payment Prediction

## Dataset
- Source: `data/staging/psav_model_2025-01-01_to_2026-01-01_pulled_2026-03-06.parquet`
- Working sample size: 1,000,000 rows
- Target: `paid_flag`
- Target balance: intentionally 50/50 paid vs unpaid

## Cleaning decisions
- Dropped `zipcode5` because values were not reliable enough to use now.
- Dropped `zz1_parentgroupid_bit` in favor of `parent_client_code`.
- Dropped `totalallowed` because it was constant zero and had no predictive value.
- Dropped `zz1_product_group_id_op1`, `zz1_mda_product_id_op1`, and `zz1_solution_id_op1` because they were redundant hierarchy codes for `zz1_acctproduct_op1`.
- Dropped potential leakage fields:
  - `payments_sum`
  - `refunds_sum`
  - `over_payments_sum`
  - `writeoffs_sum`
  - `adjustments_sum`
  - `cancellations_sum`

## Imputation decisions
- `placeofservice` → `"unknown"`
- `reasonableandcustomary` → `0`
- `mpi_surprise_bill_ind` → `"unknown"`

## Feature observations
### Reasonable and customary
- Higher central tendency for paid claims than unpaid claims.
- Distributions overlap, so the feature appears predictive rather than deterministic.
- Kept for modeling.

### Billed amount sum
- Central tendency differs by target class.
- Distributions overlap substantially.
- Kept for modeling.

### Noncovered charges
- Very heavily right-skewed.
- Zero-inflated: at least 75% of rows are zero.
- Extreme outliers exist.
- Kept for modeling, with feature engineering planned:
  - `has_noncoveredcharges`
  - `log_noncoveredcharges`

## Distribution findings
- Multiple financial variables are strongly right-skewed and contain extreme outliers.
- `log1p` transforms were recommended for:
  - `billed_amount_sum`
  - `reasonableandcustomary`
  - `noncoveredcharges`

## Modeling implications
- Tree-based models are a strong baseline choice.
- High-cardinality categorical variables are good candidates for target encoding:
  - `cpt`
  - `providergroupname`
  - `client_code`
  - `zz1_acctproduct_op1`
  - `claim_type`
  - `provider_state`

## Next steps
1. Apply non-leaky feature engineering.
2. Split train/test before target encoding.
3. Apply K-fold target encoding on training data only.
4. Train baseline models and compare performance.
