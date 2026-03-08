#%%
# import packages 
import pandas as pd 
import numpy as np
import os
# Set maximum number of viewable rows and columns  
pd.set_option('display.max_rows', 800)
pd.set_option('display.max_columns', 100)



#%%
def summary_stats_by_pcode(file_path):

    ''' 
    The function takes in a file and calculates the total charges, total savings,
    total claim count and provides a revenue estimate by payment status. 

    '''
     # Read in data
    df = pd.read_csv(file_path, compression='bz2', sep="|", encoding='utf-8')

    # Convert to date time fields
    df['PROCESSED_DT'] = pd.to_datetime(df['PROCESSED_DT'], errors='coerce')

    # Create Month-Year formatted columns for processed date and payment date
    df['PROCESSED_DT_MONTH_YEAR'] = df['PROCESSED_DT'].dt.to_period('M')  
  
    # Create separate FeeAmount columns for lower and upper estimates
    df['BNR_FEEAMOUNT_LOWER'] = df['BNR_FEEAMOUNT'].replace(' ', np.nan).astype(float)
    df['BNR_FEEAMOUNT_UPPER'] = df['BNR_FEEAMOUNT'].replace(' ', np.nan).astype(float)

    # Calculate the smallest FeeAmount greater than zero across the entire dataset
    min_fee = df[(df['PAYMENT_STATUS']=='PAID') & (df['BNR_FEEAMOUNT_LOWER']>0)]['BNR_FEEAMOUNT_LOWER'].min()


    paid_df = df[(df['PAYMENT_STATUS']=='PAID') & (df['BNR_FEEAMOUNT_LOWER']>0)]

    # Group by network and fee amount, then calculate the size of each group
    group_sizes = paid_df.groupby(['LAST_CLOSURE_NETWORK_CD', 'BNR_FEEAMOUNT']).size()
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
    grouped = df.groupby(['PAYMENT_STATUS']).agg(
        Total_Records = ('CLAIM_ID', 'count'),
        Total_Charges = ('TOTAL_CHARGE_AMT', 'sum'),
        Total_Savings = ('SAVINGS_AMT', 'sum'),
        Total_Revenue_Estimate_Lower = ('Revenue_Estimate_LOWER', 'sum'),
        Total_Revenue_Estimate_Upper = ('Revenue_Estimate_UPPER', 'sum')
        ).reset_index()

    # Calculate the percentage of Paid and Unpaid records
    total_records = grouped['Total_Records'].sum()
    grouped['Percentage'] = (grouped['Total_Records']/total_records)
    
    # Set payment status as the index
    grouped.set_index('PAYMENT_STATUS', inplace=True)

    return grouped
#%%
import os

file_paths = [
    '/mnt/data/Fee_Recovery_2024/ANTFIOTHR-P/filtered_cleaned/ANTFIOTHR-P_merged_BNR_MPI_2024-01-01_2024-12-31_2025-01-09.nodupes.filtered_cleaned.csv.bz2',
'/mnt/data/Fee_Recovery_2024/USNAS-P/filtered_cleaned/USNAS-P_merged_BNR_MPI_2024-01-01_2024-12-31_2025-01-09.nodupes.filtered_cleaned.csv.bz2',
'/mnt/data/Fee_Recovery_2024/CIGSAR-P/filtered_cleaned/CIGSAR-P_merged_BNR_MPI_2024-01-01_2024-12-31_2025-01-09.nodupes.filtered_cleaned.csv.bz2',
'/mnt/data/Fee_Recovery_2024/SEN-P/filtered_cleaned/SEN-P_merged_BNR_MPI_2024-01-01_2024-12-31_2025-01-09.nodupes.filtered_cleaned.csv.bz2',
]
for file_path in file_paths:
    # Extract the pcode dynamically from the file path
    pcode = os.path.basename(os.path.dirname(os.path.dirname(file_path)))


    final  = summary_stats_by_pcode(file_path)
    print(f'{pcode} ran.')

    output_path = f"/mnt/data/Fee_Recovery_2024/test/{pcode}_summary_stats.xlsx"
    final.to_excel(output_path, index=True)
#%%

# %%
