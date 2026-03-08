"""
Module selects columns of interest for processing. These columns are prepared for processing based on data type. 
Categorical features are converted to numeric boolean 0 and 1 entries. Zipcode is truncated to the first three digits.
The code reads in a list of PCODEs from a CSV file. The processed factors files are output into the filtered_cleaned directories for 
their respective PCODEs.

"""
import os
import sys
import pandas as pd
import numpy as np
import sklearn
from sklearn.preprocessing import OneHotEncoder

pd.set_option("display.max_rows", 1000)
pd.set_option("display.expand_frame_repr", True)
pd.set_option('display.width', 1000)

########################################### Fille Processing  ###################################
def find_file_with_ending(directory, ending):
    # Initialize a variable to store the file name
    matched_file = None
    
    # Loop through all files in the given directory
    for filename in os.listdir(directory):
        # Check if the file name ends with the specified string
        if filename.endswith(ending):
            matched_file = filename
            break  # Stop after finding the first match
    
    return matched_file

###################################### Batch Processing script ###################################
def process_chunk(chunk, encoder, is_first_chunk, target_of_int):
    # Skip empty chunks
    if chunk.empty:
        print("Chunk is empty, skipping processing.")
        return pd.DataFrame() # Return empty dataframe if chunk is empty

    # Zipcode processing
    chunk['ZIP3'] = chunk['PROVIDER_ZIP_CODE'].astype(str).str[:3]

    # Define keep_cols
    clm_ind_vars = [x for x in list(chunk.columns) if 'CLAIM_INDICATOR_' in x]

    keep_cols = ['ALLOWED_CHARGE_AMT',
                            'APPEALED_FLAG',
                            'CLAIM_ID',
                            'CLAIM_INDICATOR',
                            'CLAIM_SPECIALTY',
                            'CLAIM_TYPE',
                            'CLIENT_CD',
                            'CONTRACT_TYPE',
                            'LAST_CLOSURE_NETWORK_CD',
                            'MBM_VALID_CHARGES',
                            'MPI_SURPRISE_BILL_IND',
                            'PARENTCLIENTCODE',
                            'POS_DESCRIPTION_HCE',
                            'PROVIDER_STATE',
                            'PROVIDER_STATE_RENDERING',
                            'PROVIDER_ZIP_CODE',
                            'PROVIDERGROUPNAME',
                            'REASONABLE_AND_CUSTOMARY_AMT',
                            'RENDERING_PROVIDER_TIN',
                            'SAVINGS_AMT',
                            'TOTAL_CHARGE_AMT',
                            'TRANSACTIONTYPE',
                            'ZIP3'] + clm_ind_vars

    if target_of_int == 'paid':
        keep_cols.append('PAYMENT_STATUS')
        side_cols = ['PAYMENT_STATUS','CLAIM_ID']
    elif target_of_int == 'allowable_used':
        keep_cols.append('MultiPlanAllowableUsed')
        side_cols = ['MultiPlanAllowableUsed','CLAIM_ID']

    chunk['SAVINGS_RATE'] = np.where(chunk['TOTAL_CHARGE_AMT'] != 0, chunk['SAVINGS_AMT']/chunk['TOTAL_CHARGE_AMT'], 0)

    chunk['CLAIM_ID'] = chunk['CLAIM_ID'].astype(str)

    # Filter for keep_cols
    chunk = chunk[keep_cols]
        
    contin_features = list(chunk._get_numeric_data().columns)       
    cat_features = list(chunk.dtypes[chunk.dtypes == 'object'].index)
    cat_features = [ele for ele in cat_features if ele not in side_cols]

    # Handle NaNs and infinite values   
    chunk = chunk.replace([np.inf, -np.inf], 1)
    chunk = chunk.replace([np.nan], 0)

    # Convert floats to int
    for col in chunk.select_dtypes(include=['float64']).columns:
        chunk[col] = chunk[col].astype(int)   

    # Handle categorical features
    cat_df = chunk[cat_features]
    cat_df = cat_df.astype(str).replace('nan', np.nan)
    side_df = chunk[side_cols]
    chunk = chunk.drop(side_cols, axis=1)

    # One-hot encoding of categorical features
    if is_first_chunk:
        encoder.fit(cat_df)
    cat_df = encoder.transform(cat_df)
    cat_df = pd.DataFrame(cat_df, columns=encoder.get_feature_names_out(cat_features), index=chunk.index)

    # Concatenate categorical and continous features
    chunk = pd.concat([cat_df, chunk[contin_features]], axis=1)
    chunk['CLAIM_ID'] = side_df['CLAIM_ID'].astype(str)

    # Handle target features
    if target_of_int == 'allowable_used':
        side_df['MultiPlanAllowableUsed'] = side_df['MultiPlanAllowableUsed'].str.upper().str.strip()
        try:
            train_target = pd.get_dummies(side_df['MultiPlanAllowableUsed']).YES
        except:
            train_target = pd.get_dummies(side_df['MultiPlanAllowableUsed']).NO
            train_target[train_target == 1] = 0
    elif target_of_int == 'paid':
        # Checks if there are paid claims, if not, sets all to unpaid
        try:
            train_target = pd.get_dummies(side_df['PAYMENT_STATUS']).PAID
        except:
            train_target = side_df['PAYMENT_STATUS']
            train_target[:] = 0
    chunk['target'] = train_target

    return chunk


################################ ################################################

def create_factors(pcode = '', target_of_int = 'paid', chunk_size=100000):
    pcodes_file = pd.read_csv('/mnt/data/Fee_Recovery_2024/temp/remaining_trees/pcodes.csv')
    pcodes_to_run = list(pcodes_file['pcode'])
    for current_pcode in pcodes_to_run:
        data_path= f'/mnt/data/Fee_Recovery_2024/{current_pcode}/filtered_cleaned'
        file_name = find_file_with_ending(data_path, 'nodupes.filtered_cleaned.csv.bz2')
        data_path = data_path + '/' + file_name
        output_path = data_path[:-4] + ".factors.csv"

        encoder = OneHotEncoder(sparse=False, handle_unknown='ignore')

        # Process chunks and write directly to file
        is_first_chunk = True
        for chunk in pd.read_csv(data_path, delimiter='|', na_values='NA', keep_default_na=False, low_memory=False, chunksize=chunk_size):

            # Process each chunk
            processed_chunk = process_chunk(chunk, encoder, is_first_chunk, target_of_int)

            # Write the processed chunk to file
            mode = 'w' if is_first_chunk else 'a'
            header = is_first_chunk
            processed_chunk.to_csv(output_path + '.bz2', sep='|', index=False, compression='bz2', mode=mode, header=header)

            is_first_chunk = False
        print(f"File processed and saved for pcode: {current_pcode}!")

if __name__ == "__main__":
    create_factors() # Call the function without command-line arguments




