#%%
import pandas as pd

file_path = '/mnt/data/Fee_Recovery_2024/HCMA-P/filtered_cleaned/HCMA-P_merged_BNR_MPI_2024-01-01_2024-12-31_2025-04-01.nodupes.filtered_cleaned.csv.bz2.unpaid.csv'

df = pd.read_csv(file_path, delimiter = ',')


# %%
pd.set_option('display.max_columns', None)
df.head()
# %%

# Save to CSV
df.to_csv('/mnt/code/Benchmarking/HCMA-P_merged_BNR_MPI_2024-01-01_2024-12-31_2025-04-01.nodupes.filtered_cleaned.csv.bz2.unpaid.csv')
# %%
