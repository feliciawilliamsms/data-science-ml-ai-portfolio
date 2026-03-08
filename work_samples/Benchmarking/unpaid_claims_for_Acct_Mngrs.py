## want to dump paid claims
## only subset of columns that will be meaningful to AMs
## dump to AM folder
## read in claim file and AM association file
##
## python zz_dump_file.py claim_file.bz2

# import packages 
import pandas as pd 
import argparse
import os
import sys
from dateutil.parser import parse as date_parser

# #am_file = sys.argv[1]
# claim_file = sys.argv[1]

def unpaid_claims_for_AM(input_file_path):
#ams = pd.read_csv(am_file)
    claims = pd.read_csv(input_file_path, delimiter = '|', na_values='NA', keep_default_na=False, low_memory=False)

    claims['ZIP3'] = claims['PROVIDER_ZIP_CODE'].astype(str).str[:3]

    keep_cols = ['CLIENT_CD','CLAIM_ID','SUBMITTERCLAIMNUMBER','CLIENT_ADJUSTMENT_CLAIM_NUMBER',
                 'FIRST_DOS','RECEIVED_DT','MEMBER_ID','PATIENT_FIRST_NM','PATIENT_LAST_NM','PATIENT_BIRTH_DT',
                 'SOURCE_CD','TOTAL_CHARGE_AMT','SAVINGS_AMT','ALLOWED_CHARGE_AMT',
                                'APPEALED_FLAG',
                                'CLAIM_INDICATOR',
                                'CLAIM_SPECIALTY',
                                'CLAIM_TYPE',
                                'CLIENT_NM',
                                'CONTRACT_TYPE',
                                'LAST_CLOSURE_NETWORK_CD',
                                'MBM_VALID_CHARGES',
                                'MPI_SURPRISE_BILL_IND',
                                'PARENTCLIENTCODE',
                                'POS_DESCRIPTION_HCE',
                                'CITY_NM',
                                'PROVIDER_STATE',
                                'PROVIDER_STATE_RENDERING',
                                'PROVIDER_COUNTY_NAME',
                                'PROVIDER_ZIP_CODE',
                                'PROVIDERGROUPNAME',
                                'REASONABLE_AND_CUSTOMARY_AMT',
                                'RENDERING_PROVIDER_TIN',
                                'ORIGINAL_PROVIDER_TAX_ID',
                                'PROVIDER_NPI',
                                'BNR_TRANSACTIONTYPE',
                                'ZIP3', 'PAYMENT_STATUS']
            
    claims = claims.loc[(claims['PAYMENT_STATUS']=='UNPAID') & (claims['SAVINGS_AMT']>0)][keep_cols]
    
    # claims = claims.loc[(claims['RECEIVED_DT']>'2023-12-31') & (claims['RECEIVED_DT']<'2024-11-25')] # changed the end date --- altered the script to the one below FRW
    claims = claims.loc[(claims['RECEIVED_DT']>'2023-12-31')] # changed the end date 
    
    
    claims['CLAIM_ID'] = (claims['CLAIM_ID'].astype(str))
    claims['SUBMITTERCLAIMNUMBER'] = (claims['SUBMITTERCLAIMNUMBER'].astype(str))
    claims['CLIENT_ADJUSTMENT_CLAIM_NUMBER'] = (claims['CLIENT_ADJUSTMENT_CLAIM_NUMBER'].astype(str))

    return claims

def parse_date(date_str):
    try:
        return date_parser(date_str).strftime("%Y-%m-%d") 
    except (TypeError, ValueError):
        return pd.NaT

if __name__=="__main__":
    arg_parser =  argparse.ArgumentParser(description =' Process start and end dates.')
    arg_parser.add_argument('--start_date', help='Start date in YYYY-MM-DD format', required=True)
    arg_parser.add_argument('--end_date', help='End date in YYYY-MM-DD format', required=True)
    arg_parser.add_argument('--pull_date', help='Date of data dump in YYYY-MM-DD format', required=True)
    arg_parser.add_argument('--pcode', help='PCODE', required=True)

    args = arg_parser.parse_args()
   
    # Parse the dates & Parent Client Code
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    pull_date = parse_date(args.pull_date)
    pcode = args.pcode

    # Input file path
    input_file_path =  f'/mnt/data/Fee_Recovery_2024/{pcode}/filtered_cleaned/{pcode}_merged_BNR_MPI_{start_date}_{end_date}_{pull_date}.nodupes.filtered_cleaned.csv.bz2'
    input_file_name = input_file_path[input_file_path.rindex('/')+1:]
    print(f"Working on: {input_file_name}")

    # Run the unpaid claims for account managers function
    data = unpaid_claims_for_AM(input_file_path)

    # Output file path
    output_path = input_file_path + ".unpaid.csv"

    # Export file as csv
    data.to_csv(output_path, sep=',', index=False)
    
    print(f"Unpaid claims saved to: {output_path}")