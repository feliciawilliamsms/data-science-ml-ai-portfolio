#%%

## run this by python booking_table_FILE.py file_of_interest.csv
## note, input file is csv 
## NOTE: pass in the file you want the booking curve for, ie if a specific leaf datafile must be for that leaf

## fancy for i in *_bucket.csv ; do python booking_table_FILE.py $i ; done


# import packages 
import pandas as pd 
import argparse
import os
import sys
from dateutil.parser import parse as date_parser


def parse_date(date_str):
    try: 
        return parser.parse(date_str, default=None)
    except (TypeError, ValueError):
        return pd.NaT

def calculate_booking_table_with_totals(file_path):


    # Initialize an empty list to store chunks
    chunks = pd.read_csv(file_path, chunksize=1000, compression='bz2', sep="|", encoding='utf-8')

    # Concatenate chunks while handling malformed dates
    df = pd.concat([chunk for chunk in chunks], ignore_index=True)

    # Drop the 'Unamed: 0' column if it exists
    if 'Unnamed: 0' in df.columns:
        df.drop(columns=['Unnamed: 0'], inplace=True)

    # Claims with savings that have potential for payment
    df = df[df['SAVINGS_AMT']>0]

    # Insure processed date is in string format before replacing zeros with missing values
    df['PROCESSED_DT'] = df['PROCESSED_DT'].astype('str')
    
    # Replace 0's with NaN in date columns
    df['PROCESSED_DT'].replace('0', pd.NaT, inplace=True) 
    df['BNR_ENTRYDATE'].replace('0', pd.NaT, inplace=True) 

    # Apply parse_date to date columns
    df['PROCESSED_DT'] = df['PROCESSED_DT'].apply(parse_date)
    df['BNR_ENTRYDATE'] = df['BNR_ENTRYDATE'].apply(parse_date)

    # Convert to date time fields
    df['PROCESSED_DT'] = pd.to_datetime(df['PROCESSED_DT'])
    df['BNR_ENTRYDATE'] = pd.to_datetime(df['BNR_ENTRYDATE'])

    # Remove potential resubmits - removes unpaid claims have to find a different way to filter out resubmits
    df = df[(df['BNR_ENTRYDATE'] > df['PROCESSED_DT']) | df['BNR_ENTRYDATE'].isnull()] 

    # Create Month-Year formatted columns for processed date and payment date
    df['PROCESSED_DT_MONTH_YEAR'] = df['PROCESSED_DT'].dt.to_period('M')
    df['BNR_ENTRYDATE_MONTH_YEAR'] = df['BNR_ENTRYDATE'].dt.to_period('M')

    # Group by start_year_month and Pyament _Status to calculate payments by year month
    PAYMENT_STATUS_by_year_month = df.groupby(['PROCESSED_DT_MONTH_YEAR', 'PAYMENT_STATUS']).agg({'CLAIM_ID':'count','SAVINGS_AMT':'sum'}).reset_index()

    # Pivot the table so that payment status levels become columns
    result = PAYMENT_STATUS_by_year_month.pivot_table(index='PROCESSED_DT_MONTH_YEAR', columns='PAYMENT_STATUS', values=['CLAIM_ID', 'SAVINGS_AMT'], fill_value=0)

    # Flatten the multi-level column indexs
    result.columns = ['_'.join(col) for col in result.columns.values]
    
    # Conditional script to handle cases when 100% paid or 100% unpaid claims exist.
    if 'CLAIM_ID_PAID' in result.columns and 'CLAIM_ID_UNPAID' in result.columns:
        # Calculate total records
        result['Total_Records'] = result['CLAIM_ID_PAID'] + result['CLAIM_ID_UNPAID']      
        # Calculate pay rate
        result['Pay_Rate'] = result['CLAIM_ID_PAID']/result['Total_Records']
        
        result.columns = ['Paid_Claim_Count', 
        'Unpaid_Claim_Count', 
        'Paid_Savings_Total',
        'Unpaid_Savings_Total', 
        'Total_Claim_Count', 
        'Pay_Rate']
    
    elif 'CLAIM_ID_PAID' in result.columns and 'CLAIM_ID_UNPAID' not in result.columns:
        print("Warning: One or both of the expected columns 'CLAIM_ID_PAID or CLAIM_ID_UNPAID not found.")
        # Calculate total records
        result['Total_Records'] = result.get('CLAIM_ID_PAID',0) + result.get('CLAIM_ID_UNPAID',0) # Uses zero if either of the Paid or Unpaid is missing      
        # Calculate pay rate
        result['Pay_Rate'] = result.get('CLAIM_ID_PAID',0)/result['Total_Records']

        result.columns = [
        'Paid_Claim_Count', 
        'Paid_Savings_Total', 
        'Total_Claim_Count', 
        'Pay_Rate']
    else:
        print("Warning: One or both of the expected columns 'CLAIM_ID_PAID or CLAIM_ID_UNPAID not found.")
        # Calculate total records
        result['Total_Records'] = result.get('CLAIM_ID_PAID',0) + result.get('CLAIM_ID_UNPAID',0) # Uses zero if either of the Paid or Unpaid is missing      
        # Calculate pay rate
        result['Pay_Rate'] = result.get('CLAIM_ID_PAID',0)/result['Total_Records']

        result.columns = [
        'Unpaid_Claim_Count', 
        'Unpaid_Savings_Total', 
        'Total_Claim_Count', 
        'Pay_Rate']        
    

    # Pivot table to count claims by process date and payment entry dates
    total_counts = df.pivot_table(index='PROCESSED_DT_MONTH_YEAR', columns='BNR_ENTRYDATE_MONTH_YEAR', values='CLAIM_ID', aggfunc='count', fill_value=0)

    # Calculate the total claim counts for each processed date
    count_of_all_claims = df.groupby(['PROCESSED_DT_MONTH_YEAR']).agg({'CLAIM_ID':'count'}).reset_index()

    # Calculate the percent of total claims for each processed date entry date combination
    percent_of_total = round(total_counts.div(count_of_all_claims.set_index('PROCESSED_DT_MONTH_YEAR')['CLAIM_ID'], axis=0)*100,1)

    Final = pd.concat([result, percent_of_total], axis=1)

    return Final

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
    filtered_input_file_path =  f'/mnt/data/Fee_Recovery_2024/{pcode}/filtered_cleaned/{pcode}_merged_BNR_MPI_{start_date}_{end_date}_{pull_date}.nodupes.filtered_cleaned.csv.bz2'
    filtered_input_file_name = filtered_input_file_path[filtered_input_file_path.rindex('/')+1:]
    print(f"Working on: {filtered_input_file_name}")
    

    # Run the summary stats function
    data = calculate_booking_table_with_totals(filtered_input_file_path)

    # Output file path
    booking_output_file_path = f"/mnt/data/Fee_Recovery_2024/{pcode}/booking_curve/{pcode}_booking_table_{start_date}_{end_date}_{pull_date}.csv"
    
    # Export file as CSV.bz2 compressed file
    data.to_csv(booking_output_file_path, index=True)
    
    print(f"Time to payment or booking table saved to {booking_output_file_path}")
# %%

# Here are the errors that I am seeing for the booking table file. There is a problem with the CLAIM_ID_PAID Column to troubleshoot. 

# Traceback (most recent call last):
#   File "/mnt/code/Benchmarking/booking_table_FILE.py", line 125, in <module>
#     data = calculate_booking_table_with_totals(filtered_input_file_path)
#   File "/mnt/code/Benchmarking/booking_table_FILE.py", line 72, in calculate_booking_table_with_totals
#     result['Total_Records'] = result['CLAIM_ID_PAID'] + result['CLAIM_ID_UNPAID']
#   File "/opt/conda/lib/python3.9/site-packages/pandas/core/frame.py", line 3805, in __getitem__
#     indexer = self.columns.get_loc(key)
#   File "/opt/conda/lib/python3.9/site-packages/pandas/core/indexes/base.py", line 3802, in get_loc
#     raise KeyError(key) from err
# KeyError: 'CLAIM_ID_PAID'