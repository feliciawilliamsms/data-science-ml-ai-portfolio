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
import glob
from tkinter.filedialog import askdirectory
import sys
from sklearn import preprocessing
from sklearn.preprocessing import StandardScaler
from random import sample
from numpy.random import uniform
import numpy as np
from math import isnan
from datetime import date
pd.set_option("display.max_rows", 1000)
pd.set_option("display.expand_frame_repr", True)
pd.set_option('display.width', 1000)

########################################### Part 1 ###################################
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

################################################## Part 2 ################################################
def create_factors(pcode = '', target_of_int = 'paid'):
    pcodes_file = pd.read_csv('/mnt/data/Fee_Recovery_2024/temp/remaining_trees/pcodes.csv')
    pcodes_to_run = list(pcodes_file['pcode'])

    for pcode in pcodes_to_run:
        data_path= f'/mnt/data/Fee_Recovery_2024/{pcode}/filtered_cleaned'

        file_name = find_file_with_ending(data_path, 'nodupes.filtered_cleaned.csv.bz2')
        data_path = data_path + '/' + file_name
        output_path = data_path[:-4] + ".factors.csv"
        ############# Low Memory = False is different from the original
        train_data = pd.read_csv(data_path, delimiter = '|', na_values='NA', keep_default_na=False, low_memory=False)
        if train_data.shape[0] == 0:
            continue
################################################## Part 3 ################################################
        train_data['ZIP3'] = train_data['PROVIDER_ZIP_CODE'].astype(str).str[:3]
        clm_ind_vars = [x for x in list(train_data.columns) if 'CLAIM_INDICATOR_' in x]

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
################################################## Part 4 ################################################                               
        if target_of_int == 'paid':
            keep_cols.append('PAYMENT_STATUS')
            side_cols = ['PAYMENT_STATUS','CLAIM_ID']
        elif target_of_int == 'allowable_used':
            keep_cols.append('MultiPlanAllowableUsed')
            side_cols = ['MultiPlanAllowableUsed','CLAIM_ID']

################################################## Part 5 ################################################
        train_data['SAVINGS_RATE'] = np.where(train_data['TOTAL_CHARGE_AMT'] != 0, train_data['SAVINGS_AMT']/train_data['TOTAL_CHARGE_AMT'], 0)
        train_data = train_data[keep_cols]

        #data = full_data.merge(train_data,how='right', on='CLAIM_ID')
        train_data['CLAIM_ID'] = train_data['CLAIM_ID'].astype(str)
        
        contin_features = list(train_data._get_numeric_data().columns)
        

        cat_features = list(train_data.dtypes[train_data.dtypes == 'object'].index)
        cat_features = [ele for ele in cat_features if ele not in side_cols]
        num_cols = train_data._get_numeric_data().columns
        train_data = train_data.replace([np.inf, -np.inf], 1)
        train_data = train_data.replace([np.nan], 0)
################################################## Part 6 ################################################
        # Convert floats to ints
        for col in train_data.select_dtypes(include=['float64']).columns:
            print(col)
            train_data[col] = train_data[col].astype(int)
        cat_df = train_data[cat_features]
        cat_df = cat_df.astype(str).replace('nan',np.nan)
        side_df = train_data[side_cols]
        train_data = train_data.drop(side_cols, axis=1)
################################################## Part 7 ################################################
        #Initialize OneHotEncoder
        encoder = OneHotEncoder(sparse=False)
        # Apply one-hot encoding to the categorical columns
        cat_df = encoder.fit_transform(cat_df)

        cat_df = pd.DataFrame(cat_df, columns=encoder.get_feature_names_out(cat_features), index=train_data.index)
        # cat_df = pd.DataFrame(cat_df, index=train_data.index)

        train_data = pd.concat([cat_df, train_data[contin_features]], axis=1)
        train_data['CLAIM_ID'] = side_df['CLAIM_ID']

################################################## Part 8 ################################################
        # Create target variable
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
        train_data['target'] = train_target
        print(output_path)
        train_data.to_csv(output_path +  '.bz2', sep='|', index=False, compression='bz2')
        print("file done!")
################################################## Part 9 ################################################
if __name__ == "__main__":
    create_factors() # Call the function without command-line arguments




