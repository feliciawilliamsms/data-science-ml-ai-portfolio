############## create and dump file with fully expanded factors
##
## do this in the filtered_cleaned directory, it will create factor files in the ../decision_trees/ directory
## python /mnt/code/factor_creation.py <input_file>
##
## python /mnt/code/filtering_cleaning.py x000_test.csv.filtered_cleaned.csv
##
## target_of_int = 'paid' OR 'allowable_used'
##
## simply one-hot all necessary columns
## TODO: EDA to figure out if there are any continuous variables that can be factorized for other models
##
## output a single file containing a fully expanded data frame in the decision_trees directory
##

import os
import sys
import pandas as pd
import numpy as np
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

def create_factors(pcode = '', target_of_int = 'paid'):
    pcodes_file = pd.read_excel('/mnt/pcodes_to_run.xlsx')
    pcodes_to_run = list(pcodes_file['Pcode'])

    for pcode in pcodes_to_run:
        data_path= '/domino/datasets/Fee_Recovery_2024/' + pcode + '/filtered_cleaned'

        file_name = find_file_with_ending(data_path, 'nodupes.filtered_cleaned.csv.bz2')
        data_path = data_path + '/' + file_name
        output_path = data_path[:-4] + ".factors.csv"
        train_data = pd.read_csv(data_path, delimiter = '|', na_values='NA', keep_default_na=False)
        if train_data.shape[0] == 0:
            continue
        #train_data = train_data.loc[(train_data['PROCESSED_DT'] >= '2023-07-01') & (train_data['PROCESSED_DT'] <= '2024-04-01')]

        # Split continuous and categorical vars up

        # NOTE: here are the currently available columns
        #       update after new data dump

        """
        ,Unnamed: 0,BNR_TOTAL_BILLEDAMOUNT,BNR_BALANCE,BNR_ADJUSTEDTOTAL,BNR_BILLDATE,BNR_SAVINGS,BNR_AMOUNT,BNR_TRANSACTIONDATE,BNR_ENTRYDATE,BNR_DESCRIPTION,BNR_ACCOUNTID,

        CLAIM_INDICATOR,CLAIM_TYPE,CLAIM_ID,CLAIM_SPECIALTY,CLIENT_CD,CONTRACT_NAME,CONTRACT_SRC,CONTRACT_TYPE,TOTAL_CHARGE_AMT,CPT,CLAIM_FAIL_REASON_CD,ENGINE,FUNDING_TYPE,CITY_NM,CLIENT_NM,PROVIDER_COUNTY_NAME,PROCESSED_DT,STATE_CD,ZIP5,LAST_UPDATED_TS,MAGIC_SOLUTION_CLASS,MBM_VALID_CHARGES,NETWORK_CD,NON_CONTRACT_CD,NON_COVERED_CHARGE_AMT,PARENTCLIENTCODE,POS_DESCRIPTION_HCE,PRIMARY_PAYOR_CODE,PROVIDERGROUPNAME,REASONABLE_AND_CUSTOMARY_AMT,SAVINGS_AMT,ACCOUNTING_IND,RENDERING_PROVIDER_TIN,ALLOWED_CHARGE_AMT,TOTAL_CLAIM_DAYS_CNT,TYPEOFBILL,APPEALED_FLAG,TRANSACTIONTYPE,PRODUCTCODE,FEEDESCRIP,FEEAMOUNT,FEETYPE,PROVIDER_ZIP_CODE,PROVIDER_ZIP_CODE_RENDERING,PROVIDER_STATE,PROVIDER_STATE_RENDERING,CLIENT_SURPRISE_BILL_IND,MPI_SURPRISE_BILL_IND,CLAIM_QUALITY_SCORE,PATIENT_AGE_65_IND,RECEIVED_DT,SUBMITTERCLAIMNUMBER,ADJUSTMENTSTATUS,CLIENT_ADJUSTMENT_CLAIM_NUMBER,PATIENT_FIRST_NM,PATIENT_LAST_NM,ADJUSTEDCLAIMNUMBER,RN,PAYMENT_STATUS,SAVINGS_RATE
        """
        train_data['ZIP3'] = train_data['ZIP5'].astype(str).str.replace('-', '', regex=False).str.replace('.0', '', regex=False) 
        clm_ind_vars = [x for x in list(train_data.columns) if x in 'CLAIM_INDICATOR_']

        keep_cols = ['APPEALED_FLAG', 'CLIENT_CD', 'CLAIM_SPECIALTY','LAST_CLOSURE_NETWORK_CD',
                                'SAVINGS_AMT', 'TOTAL_CHARGE_AMT',
                                'MPI_SURPRISE_BILL_IND', 'ZIP3',
                                'PROVIDER_STATE','CLAIM_ID', 'SAVINGS_RATE'] + clm_ind_vars
        if target_of_int == 'paid':
            keep_cols.append('PAYMENT_STATUS')
            side_cols = ['PAYMENT_STATUS','CLAIM_ID']
        elif target_of_int == 'allowable_used':
            keep_cols.append('MultiPlanAllowableUsed')
            side_cols = ['MultiPlanAllowableUsed','CLAIM_ID']

        train_data['SAVINGS_RATE'] = train_data['SAVINGS_AMT']/train_data['TOTAL_CHARGE_AMT']
        train_data = train_data[keep_cols]

        #full_data = pd.read_excel('/mnt/ML-client-fee-recovery/UHN_Packages_full_data_20240618.xlsx')

        #data = full_data.merge(train_data,how='right', on='CLAIM_ID')
        train_data['CLAIM_ID'] = train_data['CLAIM_ID'].astype(str)
        
        contin_features = list(train_data._get_numeric_data().columns)
        #unwanted_vars = ['ZIP5','PROVIDER_ZIP_CODE'] 
        #contin_features = [ele for ele in contin_features if ele not in unwanted_vars ]
        cat_features = list(train_data.dtypes[train_data.dtypes == 'object'].index)
        cat_features = [ele for ele in cat_features if ele not in side_cols ]
        num_cols = train_data._get_numeric_data().columns
        train_data = train_data.replace([np.inf, -np.inf], 1)
        train_data = train_data.replace([np.nan], 0)

        # Convert floats to ints
        for y in train_data.columns:
            if(train_data[y].dtype == np.float64):
                print(y)
                train_data[y] = train_data[y].astype(int)
        cat_df = train_data[cat_features]
        cat_df = cat_df.astype(str).replace('nan',np.nan)
        side_df = train_data[side_cols]
        train_data = train_data.drop(side_cols, axis=1)

        #Initialize OneHotEncoder
        encoder = OneHotEncoder(sparse=False)
        # Apply one-hot encoding to the categorical columns
        cat_df = encoder.fit_transform(cat_df)
        cat_df = pd.DataFrame(cat_df, columns=encoder.get_feature_names_out(cat_features), index=train_data.index)

        train_data = pd.concat([cat_df, train_data[contin_features]], axis=1)
        train_data['CLAIM_ID'] = side_df['CLAIM_ID']


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

if __name__ == "__main__":
    

    try:
        pcode = sys.argv[1]
        create_factors(pcode)
    except:
        create_factors()
    """
    except:
        target_of_int = 'paid'
        create_factors(data_path = '/domino/datasets/Fee_Recovery_2024/PAM-P/filtered_cleaned') #
    """