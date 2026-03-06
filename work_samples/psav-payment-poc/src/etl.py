import pandas as pd
from sqlalchemy import text

from pathlib import Path
from datetime import date

from src.config import load_config
from src.db_connection import get_engine

STAGING_DIR = Path("data/staging")
STAGING_DIR.mkdir(parents=True, exist_ok=True)

cfg = load_config()
data_cfg = cfg["data"]

start_date = data_cfg["start_date"]
end_date = data_cfg["end_date"]

# often we use end_date as the as_of_date for reproducibility
as_of_date = end_date

fee_type = "PS"
bill_method = "PSAV"


def extract_psav_model(
    start_date: str,
    end_date: str,
    as_of_date: str,
    fee_type: str = "PS",
    bill_method: str = "PSAV",
) -> pd.DataFrame:
    engine = get_engine()

    model_sql = text(
        """
        WITH CLAIMS AS (
            SELECT
                c.*,
                TO_CHAR(c.CLAIMNUMBER) AS CLAIMNUMBER_C
            FROM ADMIN.FEE_RECOVERY_PSAV_PEPM_EXTRACT c
            WHERE c.RECEIVEDDATE >= TO_DATE(:start_date, 'YYYY-MM-DD')
              AND c.RECEIVEDDATE < TO_DATE(:end_date, 'YYYY-MM-DD')
              AND NVL(c.SAVINGS, 0) > 0
              AND c.CLAIMSTATUS = 1007
              AND c.CLAIMSTATUS <> 1009
              AND (
                    c.STATUS_CODE IS NULL
                    OR (
                        TO_CHAR(c.STATUS_CODE) NOT LIKE '800%'
                        AND TO_CHAR(c.STATUS_CODE) NOT LIKE '900%'
                    )
                  )
              AND (
                    c.PATIENTFIRSTNAME IS NULL
                    OR UPPER(c.PATIENTFIRSTNAME) NOT LIKE '%TEST%'
                  )
              AND (
                    c.PATIENTLASTNAME IS NULL
                    OR UPPER(c.PATIENTLASTNAME) NOT LIKE '%TEST%'
                  )
              AND c.MPI_CLAIM_ID IS NOT NULL
              AND c.ADJUSTMENTSTATUS IN (0, 2004)
              AND (
                    c.PATIENTDATEOFBIRTH IS NULL
                    OR FLOOR(MONTHS_BETWEEN(c.RECEIVEDDATE, c.PATIENTDATEOFBIRTH) / 12) < 65
                  )
        ),

        ACCOUNTING AS (
            SELECT
                a.*,
                TO_CHAR(a.ZZ1_RIMS_CLAIM_NUMBER_OP1) AS CLAIMNUMBER_A
            FROM ADMIN.V_BILLED_PAID_ITEMS_T a
            WHERE a.ZZ1_FEETYPE_OP1 = :fee_type
              AND a.ZZ1_BILL_METHOD_ID_OP1 = :bill_method
              AND a.ZZ1_RIMS_CLAIM_NUMBER_OP1 IS NOT NULL
        ),

        ACCOUNTING_CLAIM_LEVEL AS (
            SELECT
                CLAIMNUMBER_A,

                MAX(ZZ1_RIMS_CLAIM_NUMBER_OP1) AS ZZ1_RIMS_CLAIM_NUMBER_OP1,

                SUM(NVL(BILLED_AMOUNT, 0)) AS billed_amount_sum,
                SUM(NVL(PAYMENTS, 0)) AS payments_sum,
                SUM(NVL(OVER_PAYMENTS, 0)) AS over_payments_sum,
                SUM(NVL(REFUNDS, 0)) AS refunds_sum,
                SUM(NVL(WRITEOFFS, 0)) AS writeoffs_sum,
                SUM(NVL(ADJUSTMENTS, 0)) AS adjustments_sum,
                SUM(NVL(CANCELLATIONS, 0)) AS cancellations_sum,

                COUNT(*) AS accounting_row_count,

                MAX(CASE WHEN BLART = 'PY' THEN 1 ELSE 0 END) AS paid_flag,
                MIN(CASE WHEN BLART = 'PY' THEN BUDAT ELSE NULL END) AS first_payment_date,
                MIN(BUDAT) AS first_budat,
                MAX(BUDAT) AS last_budat,

                MAX(OPBEL) AS OPBEL,
                MAX(OPUPW) AS OPUPW,
                MAX(OPUPK) AS OPUPK,
                MAX(OPUPZ) AS OPUPZ,
                MAX(ZZ1_SUPERGROUPID_OP1) AS ZZ1_SUPERGROUPID_OP1,
                MAX(GPART) AS GPART,
                MAX(NAME_ORG) AS NAME_ORG,
                MAX(ZZ1_CLIENTCODE_BH_OP1) AS ZZ1_CLIENTCODE_BH_OP1,
                MAX(IDNUMBER) AS IDNUMBER,
                MAX(ZZ1_ACCTPRODUCT_OP1) AS ZZ1_ACCTPRODUCT_OP1,
                MAX(ZZ1_PRODUCT_GROUP_ID_OP1) AS ZZ1_PRODUCT_GROUP_ID_OP1,
                MAX(ZZ1_MDA_PRODUCT_ID_OP1) AS ZZ1_MDA_PRODUCT_ID_OP1,
                MAX(ZZ1_FEETYPE_OP1) AS ZZ1_FEETYPE_OP1,
                MAX(ZZ1_BILL_METHOD_ID_OP1) AS ZZ1_BILL_METHOD_ID_OP1,
                MAX(XBLNR) AS XBLNR,
                MAX(ABRZU) AS ABRZU,
                MAX(ABRZO) AS ABRZO,
                MAX(ZZ1_SOLUTION_ID_OP1) AS ZZ1_SOLUTION_ID_OP1,
                MAX(HERKF_KK) AS HERKF_KK,
                MAX(FIKEY) AS FIKEY,
                MAX(REV_SPECIALIST) AS REV_SPECIALIST,
                MAX(BLART) AS BLART,
                MAX(ON_ACCOUNT) AS ON_ACCOUNT,
                MAX(RATE) AS RATE,
                MAX(CENSUS) AS CENSUS,
                MAX(ZZ1_ADJUSTEDCLAIMNUM_BIT) AS ZZ1_ADJUSTEDCLAIMNUM_BIT,
                MAX(ZZ1_CLIENT_CLAIM_ID_BIT) AS ZZ1_CLIENT_CLAIM_ID_BIT,
                MAX(ZZ1_EDP_CLAIM_ID_BIT) AS ZZ1_EDP_CLAIM_ID_BIT,
                MAX(ZZ1_MPI_CLAIMID_BIT) AS ZZ1_MPI_CLAIMID_BIT,
                MAX(ZZ1_MPI_OUTBD_CLAIMID_BIT) AS ZZ1_MPI_OUTBD_CLAIMID_BIT,
                MAX(ZZ1_PRIORMPICLAIMID_BIT) AS ZZ1_PRIORMPICLAIMID_BIT,
                MAX(ZZ1_USERALPHA11_BIT) AS ZZ1_USERALPHA11_BIT,
                MAX(ZZ1_USERALPHA16_BIT) AS ZZ1_USERALPHA16_BIT,
                MAX(ZZ1_AMOUNTSAVED_BIT) AS ZZ1_AMOUNTSAVED_BIT,
                MAX(ZZ1_USERNUMBER05_BIT) AS ZZ1_USERNUMBER05_BIT,
                MAX(ZZ1_PATIENTFIRSTNAME_BIT) AS ZZ1_PATIENTFIRSTNAME_BIT,
                MAX(ZZ1_PATIENTLASTNAME_BIT) AS ZZ1_PATIENTLASTNAME_BIT,
                MAX(ZZ1_ADMISSIONDATE_BIT) AS ZZ1_ADMISSIONDATE_BIT,
                MAX(ZZ1_DISCHARGEDATE_BIT) AS ZZ1_DISCHARGEDATE_BIT,
                MAX(ZZ1_PATIENTCONTROLNO_BIT) AS ZZ1_PATIENTCONTROLNO_BIT,
                MAX(ZZ1_PROVIDERTAXIDNUM_BIT) AS ZZ1_PROVIDERTAXIDNUM_BIT,
                MAX(ZZ1_PARENTGROUPID_BIT) AS ZZ1_PARENTGROUPID_BIT,
                MAX(ZZ1_PROCESSEDDATE_BIT) AS ZZ1_PROCESSEDDATE_BIT,
                MAX(TAX_COUNTRY) AS TAX_COUNTRY,
                MAX(ZZ1_FUNDING_TYPE_BIT) AS ZZ1_FUNDING_TYPE_BIT,
                MAX(ZZ1_INSUREDID_BIT) AS ZZ1_INSUREDID_BIT,
                MAX(ZZ1_ZIPCODE_BIT) AS ZZ1_ZIPCODE_BIT,
                MAX(ZZ1_STATE_BIT) AS ZZ1_STATE_BIT

            FROM ACCOUNTING
            GROUP BY CLAIMNUMBER_A
        ),

        FINAL_MODEL AS (
            SELECT
                c.*,
                TRUNC(TO_DATE(:as_of_date, 'YYYY-MM-DD')) - TRUNC(c.RECEIVEDDATE) AS days_since_received,

                a.ZZ1_RIMS_CLAIM_NUMBER_OP1,
                a.billed_amount_sum,
                a.payments_sum,
                a.over_payments_sum,
                a.refunds_sum,
                a.writeoffs_sum,
                a.adjustments_sum,
                a.cancellations_sum,
                a.accounting_row_count,
                a.paid_flag,
                a.first_payment_date,
                a.first_budat,
                a.last_budat,

                a.OPBEL,
                a.OPUPW,
                a.OPUPK,
                a.OPUPZ,
                a.ZZ1_SUPERGROUPID_OP1,
                a.GPART,
                a.NAME_ORG,
                a.ZZ1_CLIENTCODE_BH_OP1,
                a.IDNUMBER,
                a.ZZ1_ACCTPRODUCT_OP1,
                a.ZZ1_PRODUCT_GROUP_ID_OP1,
                a.ZZ1_MDA_PRODUCT_ID_OP1,
                a.ZZ1_FEETYPE_OP1,
                a.ZZ1_BILL_METHOD_ID_OP1,
                a.XBLNR,
                a.ABRZU,
                a.ABRZO,
                a.ZZ1_SOLUTION_ID_OP1,
                a.HERKF_KK,
                a.FIKEY,
                a.REV_SPECIALIST,
                a.BLART,
                a.ON_ACCOUNT,
                a.RATE,
                a.CENSUS,
                a.ZZ1_ADJUSTEDCLAIMNUM_BIT,
                a.ZZ1_CLIENT_CLAIM_ID_BIT,
                a.ZZ1_EDP_CLAIM_ID_BIT,
                a.ZZ1_MPI_CLAIMID_BIT,
                a.ZZ1_MPI_OUTBD_CLAIMID_BIT,
                a.ZZ1_PRIORMPICLAIMID_BIT,
                a.ZZ1_USERALPHA11_BIT,
                a.ZZ1_USERALPHA16_BIT,
                a.ZZ1_AMOUNTSAVED_BIT,
                a.ZZ1_USERNUMBER05_BIT,
                a.ZZ1_PATIENTFIRSTNAME_BIT,
                a.ZZ1_PATIENTLASTNAME_BIT,
                a.ZZ1_ADMISSIONDATE_BIT,
                a.ZZ1_DISCHARGEDATE_BIT,
                a.ZZ1_PATIENTCONTROLNO_BIT,
                a.ZZ1_PROVIDERTAXIDNUM_BIT,
                a.ZZ1_PARENTGROUPID_BIT,
                a.ZZ1_PROCESSEDDATE_BIT,
                a.TAX_COUNTRY,
                a.ZZ1_FUNDING_TYPE_BIT,
                a.ZZ1_INSUREDID_BIT,
                a.ZZ1_ZIPCODE_BIT,
                a.ZZ1_STATE_BIT

            FROM CLAIMS c
            JOIN ACCOUNTING_CLAIM_LEVEL a
                ON a.CLAIMNUMBER_A = c.CLAIMNUMBER_C
        )

        SELECT *
        FROM FINAL_MODEL
        """
    )

    with engine.connect() as conn:
        return pd.read_sql(
            model_sql,
            conn,
            params={
                "fee_type": fee_type,
                "bill_method": bill_method,
                "start_date": start_date,
                "end_date": end_date,
                "as_of_date": as_of_date,
            },
        )


def save_model_dataset(df: pd.DataFrame, start_date: str, end_date: str) -> Path:
    pull_date = date.today().isoformat()
    filename = f"psav_model_{start_date}_to_{end_date}_pulled_{pull_date}.parquet"
    out_path = STAGING_DIR / filename
    df.to_parquet(out_path, index=False)
    return out_path


def run_etl() -> None:
    data_cfg = cfg["data"]

    start_date = data_cfg["start_date"]
    end_date = data_cfg["end_date"]
    as_of_date = end_date

    df_model = extract_psav_model(
        start_date=start_date,
        end_date=end_date,
        as_of_date=as_of_date,
        fee_type="PS",
        bill_method="PSAV",
    )

    out_path = save_model_dataset(df_model, start_date, end_date)

    print("Saved:", out_path)
    print("Shape:", df_model.shape)
    print("\npaid_flag distribution:")
    print(df_model["paid_flag"].value_counts(dropna=False))


if __name__ == "__main__":
    run_etl()
