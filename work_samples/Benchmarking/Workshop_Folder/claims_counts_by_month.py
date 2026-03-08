#%%
import pandas as pd 
import numpy as np
import argparse
import os
import glob
from datetime import datetime
from dateutil.parser import parse as date_parser

#%%
def summary_stats_by_pcode(file_path):
  # Read in data
    df = pd.read_csv(file_path, sep ='|', compression='bz2') 
    # Extract Parent_client code from the file path
    pcode =  os.path.basename(file_path).split('_')[0]
     # Convert to date time fields
    df['PROCESSED_DT'] = pd.to_datetime(df['PROCESSED_DT'], errors='coerce')
    df = df[(df['PROCESSED_DT'] > '2024-04-30') & (df['PROCESSED_DT'] < '2024-11-01')]

    # Create Month-Year formatted columns for processed date and payment date
    df['PROCESSED_DT_MONTH_YEAR'] = df['PROCESSED_DT'].dt.to_period('M')
 
    # Group by payment status and aggregate the required columns
    grouped = df.groupby(['PROCESSED_DT_MONTH_YEAR','PAYMENT_STATUS']).agg(
        Total_Records = ('CLAIM_ID', 'count')
        ).reset_index()
    pivot_grouped = grouped.pivot(index='PROCESSED_DT_MONTH_YEAR', columns='PAYMENT_STATUS', values='Total_Records')
    pivot_grouped['PAID'] = pivot_grouped['PAID'].fillna(0)
    pivot_grouped['UNPAID'] = pivot_grouped['UNPAID'].fillna(0)
    pivot_grouped['CLAIM_TOTAL'] = pivot_grouped['PAID'] + pivot_grouped['UNPAID']
    pivot_grouped['PAID_RATE'] = round(pivot_grouped['PAID']/pivot_grouped['CLAIM_TOTAL'],2)*100
    pivot_grouped['UNPAID_RATE'] = round(pivot_grouped['UNPAID']/pivot_grouped['CLAIM_TOTAL'],2)*100
    pivot_grouped['PCODE'] = pcode
    return pivot_grouped
    
# PCODE as a parameter
pcode_list = ['BCBSMA-P',
'WAU-P',
'CBSA-P',
'IMA-P',
'UHN-P',
'LST-P',
'HBCBS-P',
'HCSC-P',
'HSP-P',
'CWCP-P',
'AUS-P',
'GREAT-P',
'ASON-P',
'ASOL-P',
'AAE-P',
'CHPI-P',
'PSHWC-P'
]
for p in pcode_list:  
    file_path = f'/mnt/data/Fee_Recovery_2024/{p}/filtered_cleaned/{p}_merged_BNR_MPI_2024-01-01_2024-12-31_2025-01-09.nodupes.filtered_cleaned.csv.bz2'
 
    # Output file path
    output_file_path = f"/mnt/code/Benchmarking/{p}_merged_BNR_MPI_2024-01-01_2024-10-31_2025-01-09.nodupes.filtered_cleaned.csv.bz2.monthly_claims_with_pay_rate.csv"

    # Export file as CSV.bz2 compressed file
    summary_stats_by_pcode(file_path).to_csv(output_file_path, index=True)


#
# %%
