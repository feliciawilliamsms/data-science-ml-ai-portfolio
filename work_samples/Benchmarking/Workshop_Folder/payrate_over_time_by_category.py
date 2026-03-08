#%%
# import packages 
import pandas as pd 

# Set maximum number of viewable rows and columns  
pd.set_option('display.max_rows', 800)
pd.set_option('display.max_columns', 100)

file_path = '/mnt/data/Fee_Recovery_2024/CIGSAR-P/filtered_cleaned/CIGSAR-P_merged_BNR_MPI_2024-01-01_2024-07-31_2024-10-10.nodupes.filtered_cleaned.csv.bz2'

def paid_unpaid_category_counts(file_path):
    '''function takes in a file and out puts the total paid 
    and unpaid counts for each feactures subcategory along with the payrate and total count for each year-month.'''
    
    # Read in data
    df = pd.read_csv(file_path, sep ='|') 


    # Convert to date time fields
    df['PROCESSED_DT'] = pd.to_datetime(df['PROCESSED_DT'], errors='coerce')


    # Create Month-Year formatted columns for processed date and payment date
    df['PROCESSED_DT_MONTH_YEAR'] = df['PROCESSED_DT'].dt.to_period('M')

    categorical_columns = [
    'CLAIM_TYPE',
    'CLAIM_SPECIALTY',
    'CLIENT_CD',
    'PROVIDER_STATE_RENDERING',
    'LAST_CLOSURE_NETWORK_CD'
    ]

    for col in categorical_columns:

        # Pivot the table to have separate columns for 'Paid' and 'Unpaid'
        pivot_table = df.groupby(['PROCESSED_DT_MONTH_YEAR', col, 'PAYMENT_STATUS']).size().unstack(fill_value=0)
        pivot_table = pivot_table.reset_index()

        # Calculate the toal count and the paid rate
        pivot_table['Total_Count'] = pivot_table.sum(axis=1)
        pivot_table['Paid_Rate'] = pivot_table['PAID'] / pivot_table['Total_Count']

    return pivot_table


    

# %%
paid_unpaid_category_counts(file_path)
# %%
