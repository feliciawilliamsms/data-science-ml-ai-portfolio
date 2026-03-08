#%%
import pandas as pd 
import numpy as np
import argparse
import os
import glob
from datetime import datetime
from dateutil.parser import parse as date_parser


# Excel CSV File_path to fee amount rates
csv_file_path ='/mnt/code/Benchmarking/FeeAmountRates_by_PCODE_20241212.csv'

def summary_stats_by_ccode(filtered_input_file_path, csv_file_path):
    # Extract Parent_client code from the file path
    pcode =  os.path.basename(filtered_input_file_path).split('_')[0]

    # Load the main data (df1)
    df1 = pd.read_csv(filtered_input_file_path, compression='bz2', sep="|", encoding='utf-8')

    # Convert to date time fields
    df1['PROCESSED_DT'] = pd.to_datetime(df1['PROCESSED_DT'], errors='coerce')

    # Create Month-Year formatted columns for processed date and payment date
    df1['PROCESSED_DT_MONTH_YEAR'] = df1['PROCESSED_DT'].dt.to_period('M')

    # Clean and normalize PCODE columns. Later look into adding last closure network.
    df1['PARENTCLIENTCODE'] = pcode.strip().lower()

    # Load the Excel csv data
    df2 = pd.read_csv(csv_file_path, sep=',')
    df2['PARENTCLIENTCODE'] = df2['PARENT_CLIENT'].str.strip().str.lower()

    # Merge df1 and df2 on parentclientcode
    merged_df = pd.merge(df1, df2, how='left', on=['PARENTCLIENTCODE'])

    # Handle missing values after the merge
    merged_df['Average of MINIMUM_RATE'] = merged_df['Average of MINIMUM_RATE'].fillna(0)
    merged_df['Average of WEIGHTED_AVERAGE_RATE'] = merged_df['Average of WEIGHTED_AVERAGE_RATE'].fillna(0)

    # Calculate revenue estimates
    merged_df['Revenue_Estimate_Lower'] = merged_df['Average of MINIMUM_RATE']*merged_df['SAVINGS_AMT']
    merged_df['Revenue_Estimate_Upper'] = merged_df['Average of WEIGHTED_AVERAGE_RATE']*merged_df['SAVINGS_AMT']

    # Add a flag for paid records
    merged_df['Paid_Flag'] = (merged_df['FINAL_PAYMENT_STATUS'].str.lower()=='PAID').astype(int)
    
    # Group by parent client code and final payment status
    grouped = merged_df.groupby(['PROCESSED_DT_MONTH_YEAR','FINAL_PAYMENT_STATUS']).agg(
        Total_Records = ('CLAIM_ID', 'count'),
        Total_Charges=('TOTAL_CHARGE_AMT', 'sum'),
        Total_Savings=('SAVINGS_AMT', 'sum'),
        Total_Revenue_Estimate_Lower=('Revenue_Estimate_Lower','sum'),
        Total_Revenue_Estimate_Upper=('Revenue_Estimate_Upper', 'sum')
    ).reset_index()

    # Display or save the grouped summary statistics
    print(f"Processed grouped statistics for {pcode}")
    # %%
    pivoted = grouped.pivot_table(
        index='PROCESSED_DT_MONTH_YEAR', # use month-year as the index
        columns= 'FINAL_PAYMENT_STATUS', # Use payment status for columns
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

    # data = summary_stats_by_ccode(filtered_input_file_path)
    data = summary_stats_by_ccode(filtered_input_file_path, csv_file_path)

    # Output file path
    # summary_output_file_path = f"/mnt/data/Fee_Recovery_2024/{pcode}/summary_stats_2/{pcode}_summary_stats_ccode.csv"
    # Output file path
    booking_output_file_path = f"/mnt/data/Fee_Recovery_2024/{pcode}/booking_curve/{pcode}_new_booking_table.csv"
    # Export file as CSV.bz2 compressed file
    data.to_csv(booking_output_file_path, index=False)

     
    print(f"Statistics by Month saved to {booking_output_file_path}")