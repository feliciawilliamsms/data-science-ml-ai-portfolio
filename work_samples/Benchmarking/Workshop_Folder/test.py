#%%
import pandas as pd
file_path = f'/mnt/data/Fee_Recovery_2024/UHN-P/filtered_cleaned/UHN-P_merged_BNR_MPI_2024-01-01_2024-12-31_2025-04-01.nodupes.filtered_cleaned.csv.bz2'
df = pd.read_csv(file_path, sep='|')
# %%
df.sample(10)
# %%
