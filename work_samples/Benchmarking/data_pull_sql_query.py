#%%
import pandas as pd
import sqlalchemy as sqla
import os
import argparse
# from dateutil import parser as date_parser
from dateutil.parser import parse as date_parser
#%%
# Pulls Claim number from SB_Header table to use as a lookup for paid claims 
def fetch_paid_ids(engine): 
    query = "SELECT CLAIMNUMBER FROM MLMODEL.SB_CLAIM_HEADER"
    paid_ids_df = pd.read_sql_query(query, engine)
    return set(paid_ids_df['CLAIMNUMBER'])

def get_query(engine, pcode, start_date, end_date):  

    start_date_str = start_date
    end_date_str = end_date

    # SQL query to pull distinct rows based on the max date for select client. 
    claim_query = f'''
        SELECT 
        t1.*,
        CASE 
            WHEN t2.CLAIMNUMBER IS NOT NULL THEN 'PAID'
            ELSE t1.OLD_PAYMENT_STATUS
        END AS PAYMENT_STATUS  
    FROM (
        SELECT 
            TOTAL_BILLEDAMOUNT AS BNR_TOTAL_BILLEDAMOUNT,
            BALANCE AS BNR_BALANCE,
            ADJUSTEDTOTAL AS BNR_ADJUSTEDTOTAL,
            BILLDATE AS BNR_BILLDATE,
            SAVINGS AS BNR_SAVINGS,
            AMOUNT AS BNR_AMOUNT,
            TRANSACTIONDATE AS BNR_TRANSACTIONDATE,
            ENTRYDATE AS BNR_ENTRYDATE,
            DESCRIPTION AS BNR_DESCRIPTION,
            ACCOUNTID AS BNR_ACCOUNTID, 
            CLAIM_INDICATOR,
            CLAIM_TYPE,
            CLAIM_ID,
            CLAIM_SPECIALTY,
            CLIENT_CD,
            CONTRACT_NAME,
            CONTRACT_SRC,
            CONTRACT_TYPE,
            TOTAL_CHARGE_AMT,
            CPT,
            CLAIM_FAIL_REASON_CD,
            ENGINE,
            FUNDING_TYPE,
            CITY_NM,
            CLIENT_NM,
            PROVIDER_COUNTY_NAME,
            PROCESSED_DT,
            STATE_CD,
            ZIP5,
            LAST_UPDATED_TS,
            MAGIC_SOLUTION_CLASS,
            MBM_VALID_CHARGES,
            MPI_CLAIM_ID,
            NETWORK_CD,
            NON_CONTRACT_CD,
            NON_COVERED_CHARGE_AMT,
            PARENTCLIENTCODE,
            POS_DESCRIPTION_HCE,
            PRIMARY_PAYOR_CODE,
            REPLACE(PROVIDERGROUPNAME,'|','') AS PROVIDERGROUPNAME,
            REASONABLE_AND_CUSTOMARY_AMT,
            SAVINGS_AMT,
            ACCOUNTING_IND,
            RENDERING_PROVIDER_TIN,
            ALLOWED_CHARGE_AMT,
            TOTAL_CLAIM_DAYS_CNT,
            TYPEOFBILL,
            APPEALED_FLAG,
            TRANSACTIONTYPE AS BNR_TRANSACTIONTYPE,
            PRODUCTCODE,
            FEEDESCRIP AS BNR_FEEDESCRIP,
            FEEAMOUNT AS BNR_FEEAMOUNT,
            FEETYPE AS BNR_FEETYPE,
            PROVIDER_ZIP_CODE,
            PROVIDER_ZIP_CODE_RENDERING,
            PROVIDER_STATE,
            PROVIDER_STATE_RENDERING,
            CLIENT_SURPRISE_BILL_IND,
            MPI_SURPRISE_BILL_IND,
            CLAIM_QUALITY_SCORE,
            PATIENT_AGE_65_IND,
            RECEIVED_DT,
            SUBMITTERCLAIMNUMBER,
            ADJUSTMENTSTATUS,
            CLIENT_ADJUSTMENT_CLAIM_NUMBER,
            PATIENT_FIRST_NM,
            PATIENT_LAST_NM,
            ADJUSTEDCLAIMNUMBER AS BNR_ADJUSTEDCLAIMNUMBER,
            PATIENT_BIRTH_DT, 
            FIRST_DOS,
            ORIGINAL_PROVIDER_TAX_ID,
            STATUS_CODE,
            CLAIMSTATUS,
            LAST_CLOSURE_NETWORK_CD,
            POLICYNUMBER AS BNR_SELFBILLED_IND,
            SOURCE_CD,
            MEMBER_ID,
            PROVIDER_NPI,
            ROW_NUMBER () OVER (PARTITION BY CLAIM_ID ORDER BY PROCESSED_DT DESC) AS rn,
            CASE WHEN BILLDATE IS NULL THEN 'UNPAID' ELSE 'PAID' END AS OLD_PAYMENT_STATUS
            FROM MLMODEL.ML_DATA_FEE_RECOVERY
            WHERE 
                PROCESSED_DT BETWEEN TO_DATE('{start_date_str}', 'YYYY-MM-DD') AND TO_DATE('{end_date_str}', 'YYYY-MM-DD') 
                AND PATIENT_FIRST_NM != 'TEST' 
                AND PATIENT_LAST_NM != 'TEST'
                AND PARENTCLIENTCODE = '{pcode}'
    ) t1
    LEFT JOIN (
        SELECT DISTINCT CLAIMNUMBER 
        FROM MLMODEL.SB_CLAIM_HEADER
    ) t2
    ON t2.CLAIMNUMBER = t1.CLAIM_ID
    WHERE t1.rn=1   
    '''
   
    # Function to connect to and pull data from Oracle database
    df = pd.read_sql_query(claim_query, engine)
    df.columns = map(lambda x: str(x).upper(), df.columns) # Converts all column names to uppercase
    return df

# Function to parse date
def parse_date(date_str):
    try:
        return date_parser(date_str).strftime("%Y-%m-%d") # Firnat as date only
        # return date_parser.parse(date_str, default=None) 
    except (TypeError, ValueError):
        return pd.NaT


if __name__=="__main__":
    arg_parser =  argparse.ArgumentParser(description =' Process start and end dates.')
    arg_parser.add_argument('--start_date', help='Start date in YYYY-MM-DD format', required=True)
    arg_parser.add_argument('--end_date', help='End date in YYYY-MM-DD format', required=True)
    arg_parser.add_argument('--pull_date', help='Date of data dump in YYYY-MM-DD format', required=True)
    arg_parser.add_argument('--pcode', help='PCODE', required=True)
    
    args = arg_parser.parse_args()
   
    # Parse the dates & Parent Clietn Code
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    pull_date = parse_date(args.pull_date)
    pcode = args.pcode

    # Calling database credentials - user environment variable
    db_creds = os.environ['sqla_string']

    # Connection to Oracle Database using credentials
    engine=sqla.create_engine(db_creds)
        
    print(f"Working on the {pcode}")

# Fetch the data 
data = get_query(engine, pcode, start_date, end_date)

# Replace '|' wotj ' ' (empty string) in all columns
data['PROVIDERGROUPNAME'] = data['PROVIDERGROUPNAME'].str.replace('|', '', regex=False)

# Filter out rows where the patient first name or last name is 'TEST'
data = data[(data['PATIENT_FIRST_NM'] != 'TEST') & (data['PATIENT_LAST_NM'] != 'TEST')]

# Print row and column counts
print(data.shape)

# file location
csv_file_path = f"/mnt/data/Fee_Recovery_2024/{pcode}/raw/{pcode}_merged_BNR_MPI_{start_date}_{end_date}_{pull_date}.csv.bz2"

# Export file as CSV.bz2 compressed file
data.to_csv(csv_file_path, index=False, sep="|", compression='bz2', na_rep='NA')

# %%
