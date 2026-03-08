#%%
import pandas as pd 
import numpy as np
import argparse
import os
import glob
from datetime import datetime
from dateutil.parser import parse as date_parser


def summary_stats_by_ccode(file_path):
  # Read in data
    df = pd.read_csv(file_path, sep ='|') 

    # Extract Parent_client code from the file path
    pcode =  os.path.basename(filtered_input_file_path).split('_')[0]
    # Convert to date time fields
    df['PROCESSED_DT'] = pd.to_datetime(df['PROCESSED_DT'], errors='coerce')

    # Create Month-Year formatted columns for processed date and payment date
    df['PROCESSED_DT_MONTH_YEAR'] = df['PROCESSED_DT'].dt.to_period('M')
    
    # Create separate FeeAmount columns for lower and upper estimates
    df['BNR_FEEAMOUNT_LOWER'] = df['BNR_FEEAMOUNT'].replace(' ', np.nan).astype(float)
    df['BNR_FEEAMOUNT_UPPER'] = df['BNR_FEEAMOUNT'].replace(' ', np.nan).astype(float)

    # Calculate the smallest FeeAmount greater than zero across the entire dataset
    min_fee = df[(df['PAYMENT_STATUS']=='PAID') & (df['BNR_FEEAMOUNT_LOWER']>0)]['BNR_FEEAMOUNT_LOWER'].min()

    # Create paid data frame
    paid_df = df[(df['PAYMENT_STATUS']=='PAID') & (df['BNR_FEEAMOUNT_LOWER']>0)]

    # Group by network and fee amount, then calculate the size of each group
    group_sizes = paid_df.groupby(['CLIENT_CD','LAST_CLOSURE_NETWORK_CD', 'BNR_FEEAMOUNT']).size()
    print("The group sizes ran.")

    # Calculate proportions based on the total size of the paid_df
    proportions = group_sizes/paid_df.shape[0]

    # Convert proportions to a DataFrame
    proportions_df = proportions.reset_index(name='Proportion')

    # Merge the proportions back into the paid DataFrame
    weighted_avg_fee = (proportions_df['BNR_FEEAMOUNT']*proportions_df['Proportion']).sum()

    # Calculate Revenue_Estimates
    df['Revenue_Estimate_LOWER'] = min_fee*0.01*df['SAVINGS_AMT']
    df['Revenue_Estimate_UPPER'] = weighted_avg_fee*0.01*df['SAVINGS_AMT']

    # Group by payment status and aggregate the required columns
    grouped = df.groupby(['PROCESSED_DT_MONTH_YEAR','CLIENT_CD','PAYMENT_STATUS']).agg(
        Total_Records = ('CLAIM_ID', 'count'),
        Total_Charges = ('TOTAL_CHARGE_AMT', 'sum'),
        Total_Savings = ('SAVINGS_AMT', 'sum'),
        Total_Revenue_Estimate_Lower = ('Revenue_Estimate_LOWER', 'sum'),
        Total_Revenue_Estimate_Upper = ('Revenue_Estimate_UPPER', 'sum')
        ).reset_index()
# test
    # Calculate the percentage of Paid and Unpaid records
    total_record_by_CCODE = grouped.groupby('CLIENT_CD')['Total_Records'].sum().reset_index(name='CCODE_Total_Records')

    # Merge to get the total records for each CCODE
    grouped = grouped.merge(total_record_by_CCODE, on='CLIENT_CD')

    # Calculate the percentage for each CCODE and payment status
    grouped['Percentage'] = (grouped['Total_Records']/grouped['CCODE_Total_Records'])
    
    # Set payment status as the index
    grouped.drop(columns=['CCODE_Total_Records'], inplace=True)
  
    pivoted = grouped.pivot_table(
        index='PROCESSED_DT_MONTH_YEAR', # use month-year as the index
        columns= 'PAYMENT_STATUS', # Use payment status for columns
        values=[
        'Total_Records',
        'Total_Charges',
        'Total_Savings',
        'Total_Revenue_Estimate_Lower',
        'Total_Revenue_Estimate_Upper'        
        ],
        aggfunc='sum' # Aggregate function 
    )

    # Flatten the columns for cleaner naming
    pivoted.columns = [f"{col[0]} {col[1]}" for col in pivoted.columns]
    pivoted.reset_index(inplace=True)
    pivoted['PAID_RATE'] = pivoted['Total_Records PAID']/(pivoted['Total_Records PAID']+pivoted['Total_Records UNPAID'])
    pivoted['UNPAID_RATE'] = pivoted['Total_Records UNPAID']/(pivoted['Total_Records PAID']+pivoted['Total_Records UNPAID'])
    pivoted['All Claims Total'] = pivoted['Total_Records PAID']+pivoted['Total_Records UNPAID']
    
    if len(pivoted) > 3:
        avg_pay_rate = pivoted['PAID_RATE'][:-3].mean()
    else:
        avg_pay_rate = 0
    # Initialize the new column to zero
    pivoted['Remaining to be Paid Rate'] = 0
    
    # Update only the last three rows
    last_three_indices = pivoted.index[-3:] # slicing the index for the last three rows
    
    # Calculation of payrate for the last three rows.
    pivoted.loc[last_three_indices, 'Remaining to be Paid Rate'] = (
        avg_pay_rate - pivoted.loc[last_three_indices, 'PAID_RATE']
    )
    pivoted['Remaining to be Paid (Lower Estimate)'] = pivoted['Remaining to be Paid Rate']*pivoted['Total_Revenue_Estimate_Lower UNPAID']
    pivoted['Remaining to be Paid (Upper Estimate)'] = pivoted['Remaining to be Paid Rate']*pivoted['Total_Revenue_Estimate_Upper UNPAID']
    pivoted['PCODE'] = pcode 
    return pivoted
    
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
    filtered_input_file_path = f'/mnt/data/Fee_Recovery_2024/{pcode}/filtered_cleaned/{pcode}_merged_BNR_MPI_{start_date}_{end_date}_{pull_date}.nodupes.filtered_cleaned.csv.bz2'
    # filtered_input_file_path = f'/mnt/data/Fee_Recovery_2024/{pcode}/raw/{pcode}_merged_BNR_MPI_{start_date}_{end_date}_{pull_date}.csv.bz2' # From Derek's previous request
    # data = summary_stats_by_ccode(filtered_input_file_path)
    data = summary_stats_by_ccode(filtered_input_file_path)

    # Output file path
    # summary_output_file_path = f"/mnt/data/Fee_Recovery_2024/{pcode}/summary_stats_2/{pcode}_summary_stats_ccode_monthly_{start_date}_{end_date}_{pull_date}.csv"
    summary_output_file_path = f"/mnt/data/Fee_Recovery_2024/{pcode}/summary_stats_2/{pcode}_summary_stats_ccode_monthly_raw_{start_date}_{end_date}_{pull_date}.csv"
    
    # Export file as CSV.bz2 compressed file
    data.to_csv(summary_output_file_path, index=True)

     
    print(f"Summary statistics saved to {summary_output_file_path}")