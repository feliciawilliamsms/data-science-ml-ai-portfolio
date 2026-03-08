#%%
# import packages 
import pandas as pd 

# Set maximum number of viewable rows and columns  
pd.set_option('display.max_rows', 800)
pd.set_option('display.max_columns', 100)

# PCODE as a parameter
pcode = [
'ALNA-P',
'ANFINY-P',
'ARP-P',
'ASRM-P',
'CAPBC-P',
'CBH-P',
'CBHS-P',
'CHPI-P',
'CHPM-P',
'CVC-P',
'DEL-P',
'FHI-P',
'GLBA-P',
'HCMA-P',
'HCSC-P',
'HMAPS-P',
'HMASS-P',
'HSPC-P',
'IBX-P',
'KSP-P',
'MAGAM-P',
'MAGWC-P',
'RISMS-P',
'SHPG-P',
'THCPAM-P',
'VNT-P',
'VPG-P'
]

for p in pcode:
# Read in file
    file_path = f'/mnt/data/Fee_Recovery_2024/{p}/filtered_cleaned/{p}_merged_BNR_MPI_2024-01-01_2024-12-31_2025-01-09.nodupes.filtered_cleaned.csv.bz2.unpaid.csv'
    df = pd.read_csv(file_path, sep='|')
    # Convert to date time fields
    df['RECEIVED_DT'] = pd.to_datetime(df['RECEIVED_DT'], errors='coerce')
    df = df[(df['RECEIVED_DT'] > '2024-03-31') | (df['RECEIVED_DT'] < '2024-11-01')]

    # Output file path
    output_file_path = f"/mnt/code/Benchmarking/Unpaid_{p}_merged_BNR_MPI_2024-01-01_2024-10-31_2025-01-09.nodupes.filtered_cleaned.csv.bz2.unpaid.csv"
    print(f"{p} processed.")
    # Export file as CSV.bz2 compressed file
    df.to_csv(output_file_path, index=True)

# %%
