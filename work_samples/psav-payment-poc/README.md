# psav-payment-poc

Proof of concept for identifying likely payment outcomes from claims and accounting activity by combining **claims data**, **PSAV payment accounting data**, and engineered features derived from record flow and join analysis.

## Purpose

This project builds a repeatable pipeline that:

1. pulls claims data from the source database,
2. cleans and deduplicates claim-level records,
3. filters accounting data to **PSAV / PS** payment activity,
4. joins claims to accounting transactions,
5. engineers modeling features,
6. trains a baseline model, and
7. evaluates results with simple reports and plots.

The goal is to turn raw operational and financial data into a clean analytical dataset that can support payment prediction, risk scoring, or downstream payment workflow analysis.

---

## High-level workflow

```text
Database
  -> Claim extraction
  -> Claim filtering and deduplication
  -> PSAV / PS accounting filtering
  -> Join analysis between claims and accounting data
  -> Staging parquet outputs
  -> Feature engineering
  -> Model training
  -> Evaluation outputs
```

---

## Repository structure

```text
psav-payment-poc/
├── .gitignore
├── README.md
├── requirements.txt
├── .env.example
├── configs/
│   └── default.yaml
│
├── data/
│   ├── raw/
│   └── staging/
│
├── notebooks/
│   ├── 01_eda_psav_claim_payment.ipynb
│   └── 02_model_evaluation.ipynb
│
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── db_fetch.py
│   ├── etl.py
│   ├── data_cleaning.py
│   ├── feature_engineering.py
│   ├── modeling.py
│   └── model_comparison.py
│
├── reports/
│   ├── eda_summary.md
│   └── modeling_summary.md
│
└── artifacts/
    ├── figures/
    └── models/
```

---

## What each step is doing

### 1. Configuration loading (`src/config.py`)

This module loads runtime settings from:

* environment variables in `.env`, and
* pipeline parameters in `configs/default.yaml`.

Typical configuration includes:

* database credentials and DSN,
* wallet/config directory for Oracle connectivity,
* date windows for claim extraction,
* sample sizes,
* output paths, and
* baseline model hyperparameters.

This keeps logic separate from environment-specific settings.

---

### 2. Database connection and query logic (`src/db_fetch.py`)

This module is the database access layer.

It is responsible for:

* creating the SQLAlchemy engine,
* validating the database connection,
* storing the SQL text used for extraction, and
* returning raw results to the ETL process.

Conceptually, this is where the project reaches into the source system and retrieves:

* **claims data** needed for record flow analysis, and
* **accounting/payment data** relevant to PSAV / PS transactions.

---

### 3. ETL and staging (`src/etl.py`)

This is where the raw extract is converted into analysis-ready intermediate data.

The ETL step typically performs the following:

#### Claims filtering

Claims are narrowed to the population relevant to the project. Depending on the final SQL and business rules, this may include filters such as:

* date range restrictions,
* claim status restrictions,
* line-of-business or product scope,
* excluding incomplete or unusable records, and
* keeping only the fields needed for downstream analysis.

#### Claims deduplication

Claims data often contains repeated records caused by:

* multiple source rows per claim,
* repeated snapshots,
* transactional history, or
* join expansion from upstream systems.

The deduplication step is intended to produce a single reliable analytical record per claim or per claim grain used by the model.

Common deduplication approaches include:

* selecting the latest record by update timestamp,
* ranking records within each claim identifier and keeping the best candidate,
* removing exact duplicates, and
* enforcing one row per unique business key.

The exact key should match the analytical grain chosen for modeling.

#### Accounting data filtering to PSAV / PS

Accounting or payment records are filtered to the subset relevant to the PSAV payment use case.

This step isolates accounting activity associated with:

* **PSAV**,
* **PS**, or
* other approved accounting codes mapped to the payment workflow being modeled.

The purpose is to remove unrelated accounting noise and retain only the transactions that represent the payment activity of interest.

Typical filters may include:

* accounting system source,
* payment category,
* payment code or type,
* ledger/account mapping, and
* transaction date windows aligned to the claim population.

#### Join analysis

Once both sides are cleaned, the ETL process joins claims to accounting records.

This join analysis helps answer questions such as:

* Which claims have matching PSAV / PS payment activity?
* How many claims have no accounting match?
* Are there one-to-one, one-to-many, or many-to-many relationships?
* What join keys produce the most stable match rate?
* Are there timing gaps between claim activity and payment posting?

This step is important both analytically and operationally because it validates whether the source data can support the payment prediction use case.

Typical join keys might include combinations of:

* claim identifier,
* member identifier,
* provider identifier,
* payment reference,
* service dates, or
* transaction dates.

The output of this stage is usually written to `data/staging/` as parquet for repeatable downstream use.

---

### 4. Feature engineering (`src/features.py`)

After the joined dataset is staged, features are created for modeling.

The feature engineering step transforms raw claim and accounting fields into numeric, categorical, and temporal predictors.

Examples of feature categories include:

#### Claim attributes

* claim type,
* claim status,
* billed amount,
* allowed amount,
* paid amount,
* number of lines,
* provider type,
* plan or product attributes.

#### Accounting / payment attributes

* PSAV / PS indicator,
* transaction counts,
* posted payment amount,
* accounting lag,
* adjustment indicators,
* reversal or reissue indicators.

#### Join-derived features

* matched vs unmatched claim flag,
* number of accounting records linked to a claim,
* total PSAV / PS amount per claim,
* first and last transaction timing,
* duplicate-match indicators.

#### Temporal features

* days from service to adjudication,
* days from adjudication to accounting post,
* month or quarter of service,
* claim age at extraction.

#### Data quality protections

A key design principle here is **no leakage**.

That means features should only use information that would have been available at the prediction point, and should not directly encode the future outcome the model is trying to predict.

Examples of leakage to avoid:

* using final settled payment fields when predicting payment outcome,
* using post-period adjustments that occur after the prediction cutoff,
* creating labels from data that also appears as a feature.

The result is a clean feature matrix ready for model training.

---

### 5. Model training (`src/train.py`)

This module trains a baseline model using the engineered features.

Typical responsibilities include:

* train/validation split,
* fitting a baseline classifier or regressor,
* saving the fitted model,
* storing feature importance outputs when available,
* writing model artifacts to `artifacts/models/`.

At the proof-of-concept stage, the emphasis is usually on:

* establishing a reproducible baseline,
* validating signal in the joined data,
* identifying which features matter most,
* understanding whether the PSAV / PS linkage improves prediction quality.

---

### 6. Evaluation (`src/evaluate.py`)

This module produces lightweight evaluation outputs such as:

* metrics CSV files,
* summary tables,
* ROC or PR plots,
* calibration or threshold plots,
* other small PNG reports.

Outputs are written to `artifacts/reports/`.

These reports help answer:

* Does the joined PSAV / PS data improve predictive performance?
* How much usable signal exists after deduplication and filtering?
* Where do false positives or false negatives cluster?
* Which features appear to drive the baseline model?

---

## Data flow in business terms

This project can be thought of as a business-to-technical translation pipeline:

1. **Start with claims** that represent the operational population.
2. **Clean and deduplicate** them so the analytical grain is trustworthy.
3. **Filter accounting records to PSAV / PS** so only relevant payment activity remains.
4. **Join claims to payment/accounting activity** to create a unified view.
5. **Engineer features** that describe claim behavior, payment linkage, and timing.
6. **Train and evaluate a model** to test whether the combined dataset can predict the outcome of interest.

---

## Expected intermediate and final outputs

### Staging outputs

Written to `data/staging/`:

* cleaned claims parquet,
* filtered PSAV / PS accounting parquet,
* joined claim-accounting parquet,
* optionally feature-ready parquet.

### Model artifacts

Written to `artifacts/models/`:

* trained model files,
* serialized preprocessors,
* optional feature metadata.

### Reports

Written to `artifacts/reports/`:

* metrics CSV,
* plots,
* evaluation summaries.

---

## Why deduplication and join analysis matter

These are not just technical cleanup steps.

They are central to the validity of the project:

* **Deduplication** ensures the model is trained on the correct unit of analysis.
* **Filtering to PSAV / PS** ensures the target payment activity is not diluted by unrelated accounting transactions.
* **Join analysis** confirms whether claims and accounting systems align well enough to support modeling.
* **Feature engineering** converts raw system behavior into measurable predictive signal.

If any of these steps are weak, the downstream model will be less reliable or potentially misleading.

---

## Suggested execution order

A typical run order is:

```bash
python -m src.etl
python -m src.features
python -m src.train
python -m src.evaluate
```

---

## Environment notes

The project expects environment variables for database connectivity, for example:

* `ADB_USER`
* `ADB_PWD`
* `ADB_DSN`
* `ADB_WALLET_DIR`

These should be defined in a local `.env` file and should never be committed.

---

## Current project status

This repository is a proof of concept focused on validating the end-to-end path from:

* raw claims and accounting data,
* through filtering, deduplication, and joining,
* into feature engineering and baseline modeling.

As the project matures, this README can be updated with:

* exact SQL logic,
* final business definitions for PSAV / PS,
* explicit join keys,
* label definitions,
* model objective and success criteria.
