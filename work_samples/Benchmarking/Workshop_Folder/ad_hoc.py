#%%
# import packages 
import pandas as pd 

# file path
file_path = "/mnt/data/Fee_Recovery_2024/SHPH-P/filtered_cleaned/SHPH-P_merged_BNR_MPI_2024-01-01_2024-07-31_2024-10-12.nodupes.filtered_cleaned.csv.bz2.unpaid.csv"
# file_path = "/mnt/data/Fee_Recovery_2024/UHN-P/raw/UHN-P_merged_BNR_MPI_2024-01-01_2024-08-31_2024-11-18.csv.bz2"

df = pd.read_csv(file_path, sep='|')


# Output file path
output_file_path = f"/mnt/code/Benchmarking/Unpaid_SHPH-P_merged_BNR_MPI_2024-01-01_2024-07-31_2024-10-12.nodupes.filtered_cleaned.csv.bz2.unpaid.csv"

# Export file as CSV.bz2 compressed file
df.to_csv(output_file_path, index=True)
#%%
# Display all columns without truncation
pd.set_option('display.max_columns', None)

# Read in data
df = pd.read_csv(file_path, compression='bz2', sep="|", encoding='utf-8')

# View sample of 10 rows
print("-----------Data Frame Sample-------------")
print(df.sample(10))
#%%
# View number of rows and columns
print(f"Number of Columns and Rows: {df.shape}")
# List column names 
print("------------List of Column Names----------")
print(f"{df.columns}")
# %%
df['LAST_CLOSURE_NETWORK_CD'].unique()

#%%
from datetime import datetime
# Convert to date time fields
df['PROCESSED_DT'] = pd.to_datetime(df['PROCESSED_DT'], errors='coerce')

# Create Month-Year formatted columns for processed date and payment date
df['PROCESSED_DT_MONTH_YEAR'] = df['PROCESSED_DT'].dt.to_period('M')
QPA = df[df['LAST_CLOSURE_NETWORK_CD'] == 'QBP']
grouped = QPA.groupby(['PROCESSED_DT_MONTH_YEAR']).agg(
        Total_Records = ('CLAIM_ID', 'count'),
        Total_Charges = ('TOTAL_CHARGE_AMT', 'sum'),
        Total_Savings = ('SAVINGS_AMT', 'sum')
        ).reset_index()
grouped
# %%
df['MAGIC_SOLUTION_CLASS'].value_counts()
# %%

#%%
# import packages 
import pandas as pd 

# file path
file_path = "/mnt/data/Fee_Recovery_2024/UHN-P/filtered_cleaned/UHN-P_merged_BNR_MPI_2024-01-01_2024-12-31_2025-01-09.nodupes.filtered_cleaned.csv.bz2"
# file_path = "/mnt/data/Fee_Recovery_2024/UHN-P/raw/UHN-P_merged_BNR_MPI_2024-01-01_2024-08-31_2024-11-18.csv.bz2"

# Display all columns without truncation
pd.set_option('display.max_columns', None)

# Read in data
df = pd.read_csv(file_path, compression='bz2', sep="|", encoding='utf-8')
# %%
df.head()
# %%
higher_savings = df[df['SAVINGS_AMT'] > df['TOTAL_CHARGE_AMT']]
# %%
higher_savings.sample(6)
# %%
import matplotlib.pyplot as plt 
import seaborn as sns 
# Melt the DataFrame to long format
df_melted = pd.melt(higher_savings, id_vars=['PAYMENT_STATUS'], value_vars=['SAVINGS_AMT', 'TOTAL_CHARGE_AMT'],
                     var_name='Value_Type', value_name='Value')

# Create the side-by-side bar plot
sns.barplot(x='PAYMENT_STATUS', y='Value', hue='Value_Type', data=df_melted, dodge=True)

# Customize the plot (optional)
plt.title('Side-by-Side Bar Plot of Savings and Charges by Payment Status')
plt.xlabel('Category')
plt.ylabel('Dollars')
plt.legend(title='Savings & Charges')

# Show the plot
plt.show()
# %%
higher_savings.sample(8)
# %%
higher_savings['MAGIC_SOLUTION_CLASS'].value_counts()
# %%
higher_savings['POS_DESCRIPTION_HCE'].value_counts()
# %%
df['POS_DESCRIPTION_HCE'].value_counts()
# %%
value_counts = higher_savings['POS_DESCRIPTION_HCE'].value_counts()
sns.barplot(x=value_counts.index, y=value_counts.values)
plt.title('Treatment Location When Savings > Charges')
plt.xlabel('Category')
plt.ylabel('Count')
plt.xticks(rotation=90)
plt.show()
# %%
value_counts = df['POS_DESCRIPTION_HCE'].value_counts()
sns.barplot(x=value_counts.index, y=value_counts.values)
plt.title('Treatment Location Overall')
plt.xlabel('Category')
plt.ylabel('Count')
plt.xticks(rotation=90)
plt.show()
# %%
higher_savings.shape[0]/df.shape[0]
# %%
len(higher_savings)/len(df)
# %%
higher_savings['ADJUSTMENTSTATUS'].value_counts()
# %%
df['ADJUSTMENTSTATUS'].value_counts()
# %%
value_counts = higher_savings['LAST_CLOSURE_NETWORK_CD'].value_counts()
sns.barplot(x=value_counts.index, y=value_counts.values)
plt.title('Final Product Pricing When Savings > Charges')
plt.xlabel('Category')
plt.ylabel('Count')
plt.xticks(rotation=90)
plt.show()
# %%
value_counts = df['LAST_CLOSURE_NETWORK_CD'].value_counts()
sns.barplot(x=value_counts.index, y=value_counts.values)
plt.title('Final Product Pricing Overall')
plt.xlabel('Category')
plt.ylabel('Count')
plt.xticks(rotation=90)
plt.show()
# %%
