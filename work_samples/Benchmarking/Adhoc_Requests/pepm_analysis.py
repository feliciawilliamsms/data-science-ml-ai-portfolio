#%%
import pandas as pd 
# Load the data files
file1 = pd.read_excel("/mnt/data/Fee_Recovery_2024/Census_Count_NoBill_CCodes_Adam_042825.xlsx")
# file1 = pd.read_csv("Claim_Count_Bill_CCodes_Adam_042125.xlsx")

file_path = '/mnt/data/Fee_Recovery_2024/Claim_Count_NoBill_CCodes_Adam_042125.xlsx'

# View the columns names to see if renaming is neccesary for merging
excel_file =  pd.ExcelFile(file_path)
# Combine all claim count records into one dataframe
df_list = []
for sheet_name in excel_file.sheet_names:
    # Read each sheet into a data frame and append it to the list
    df = excel_file.parse(sheet_name)
    df_list.append(df)
# Concatenate all data frames into one
combined_df = pd.concat(df_list, ignore_index=True)

combined_df.head()
# %%
combined_df.shape
# %%
file1.shape
# %%
file1.columns.to_list()
# %%
combined_df.columns.to_list()
# %%
# Rename columns for easier merging
file1 = file1.rename(columns={'GroupID': 'Group_ID'})

#%%
combined_df = combined_df.rename(columns={'RIMS_CCODE': 'CCODE'})
# %%
Census_Count_Bill = file1[['ACCT_MGR', 'PCODE', 'CLIENT_NM','Group_ID', 'CENSUS', 'CENSUS_EFF_DT']]
# %%

# Merge datasets on GROUP_ID
merged_data = pd.merge(combined_df, Census_Count_Bill, on='Group_ID', how='left')

merged_data.head(6)
# %%
merged_data['Date'] = pd.to_datetime(merged_data[['Year', 'Month']].assign(DAY=1))
# %%
# Group by PCODE and Client Name and sum the claim counts
monthly_claims = merged_data.groupby(['PCODE', 'CLIENT_NM', 'Date', 'ACCTID', 'CCODE', 'CENSUS',
       'Group_ID','CENSUS_EFF_DT'], as_index=False).agg(
    Total_Claims=('Claim_Ct', 'sum')
)
# %%
monthly_claims.shape
# %%
monthly_claims.head()


#%%
monthly_claims.to_csv("/mnt/code/Benchmarking/PEPM_No_Bill_Summaries.csv", index=False)


# %%
