############## initial filter of data
##
## do this in the raw directory, it will create filtered files in the ../filtered_cleaned/ directory
## python /mnt/code/filtering_cleaning.pyy <input_file> <total charges filter> <dupe removal>
##
## python /mnt/code/filtering_cleaning.py UHN-P_merged_BNR_MPI_2022-06-01_2024-05-31_2024-06-04.csv 50000 True
##
##
## remove all claims that have 0 savings
## remove individuals > 65 years old
## do some basic cleaning:
##    fix state codes
##
## output each CCODE as filtered, cleaned files to the filtered_cleaned folder
##
## SHOULD split the input file into chunks and process independently, BUT skipping for now
## cat bigFile.csv | parallel --header : --pipe -N250000 'cat >file_{#}.csv'

import sys
import pandas as pd
import numpy as np
import re
from builtins import any as b_any
import argparse
import os
from dateutil.parser import parse as date_parser

def clean_raw_extract(file_path = '', charge_split = 0):
    ## input file as a sys.argv
    input_file = file_path # sys.argv[1]
    input_file_name = input_file[input_file.rindex('/')+1:]
    input_file_name = input_file_name.split('.', 1)[0]
    output_path = input_file.split('raw')[0] + 'filtered_cleaned/' + input_file_name
    print("File name: " + str(input_file_name))
    charge_split = round(float(charge_split)) #sys.argv[2]
    yes_filter = True


    original_claims = pd.read_csv(input_file,  compression='bz2', sep='|')
    print(original_claims.shape)
   
    ## filters
    ## filters -- switched to sequential to reduce memory footprint
    print("removing no savings")
    filtered_claims = original_claims[original_claims['SAVINGS_AMT'] > 0]
    
    ################### Temporary Removal of this filter to troubleshoot low paid claim volumes after data refresh 01/09/2025 #################
    # filter out rows where BNR_TRANSACTIONTYPE is not 'PY' or 'CR' 
    # filtered_claims = filtered_claims[~filtered_claims['BNR_TRANSACTIONTYPE'].isin(['PY','CR'])]  # Created this filter on 10/10/2024
    print(filtered_claims.shape)
    print("removing >65")
    filtered_claims = filtered_claims[filtered_claims['PATIENT_AGE_65_IND'] == 'No']
    print(filtered_claims.shape)

    print("removing PATIENT_LAST_NM == TEST")
    filtered_claims = filtered_claims[filtered_claims['PATIENT_LAST_NM'] != 'TEST']
    print(filtered_claims.shape)
    print("removing 'CLAIMSTATE' == 1009 ")
    filtered_claims = filtered_claims[filtered_claims['CLAIMSTATUS'] != 1009]
    print(filtered_claims.shape)
    print("removing 'STATUS_CODE' not like 800% ")
    # filtered_claims = filtered_claims[~filtered_claims['STATUS_CODE'].str.isin('800%')]
    print(filtered_claims.shape)
    print("removing adjustment statuses")
    filtered_claims = filtered_claims[filtered_claims['ADJUSTMENTSTATUS'].isin([0,2004])]
    print(filtered_claims.shape)
    

    ## TODO:
    # fix all the warnings b/c I am not using loc

    if filtered_claims.CONTRACT_NAME.dtype == np.float64():
        pass
    else:
        filtered_claims['CONTRACT_NAME'] = filtered_claims['CONTRACT_NAME'].str.replace('\W', '', regex=True)
    
    if filtered_claims.PROVIDER_STATE.dtype == np.float64():
        pass
    else:
        filtered_claims['PROVIDER_STATE'] = filtered_claims['PROVIDER_STATE'].str.upper()


    #  A hand full of rows will have values that cannot be read as int or float or very large values that are most likely errors
    filtered_claims = filtered_claims.replace([np.inf, -np.inf], 1)
    filtered_claims = filtered_claims.replace([np.nan], 0)
    
    # print("Calculating savings rate")
    # ## create savings rate variable
    # filtered_claims['SAVINGS_RATE'] = filtered_claims['SAVINGS_AMT']/filtered_claims['TOTAL_CHARGE_AMT']

        
    def str_detect(s, pattern):
        try:
            return bool(re.search(pattern, s))
        except: 
            return 'Missing'
    print("Creating the Claim indicator Adjustment field")
    filtered_claims['CLAIM_INDICATOR_ADJ'] = filtered_claims['CLAIM_INDICATOR'].apply(lambda x: "ADJ=Y;" if str_detect(x, "ADJ=Y;") else ("ADJ=N;" if str_detect(x, "ADJ=N;") else "NA"))

    filtered_claims['CLAIM_INDICATOR_BMP'] = filtered_claims['CLAIM_INDICATOR'].apply(lambda x: "E" if str_detect(x, "BMP=E;") else (
                                                                "EN" if str_detect(x, "BMP=EN;") else (
                                                                "EY" if str_detect(x, "BMP=EY;") else (
                                                                "N" if str_detect(x, "BMP=N;") else (
                                                                "O" if str_detect(x, "BMP=O;") else "NA")))))

    filtered_claims['CLAIM_INDICATOR_FSER'] = filtered_claims['CLAIM_INDICATOR'].apply(lambda x: "FSER=F" if str_detect(x, "FSER=F;") else "NA")

    filtered_claims['CLAIM_INDICATOR_CFR'] = filtered_claims['CLAIM_INDICATOR'].apply(lambda x: "CFR=E401" if str_detect(x, "CFR=E401;") else (
                                                                "CFR=E404" if str_detect(x, "CFR=E404;") else (
                                                                "CFR=E411" if str_detect(x, "CFR=E411;") else (
                                                                "CFR=E416" if str_detect(x, "CFR=E416;") else "NA"))))

    filtered_claims['CLAIM_INDICATOR_MRB'] = filtered_claims['CLAIM_INDICATOR'].apply(lambda x: "E" if str_detect(x, "MRB=EF;") else (
                                                                "EN" if str_detect(x, "MRB=EN;") else (
                                                                "EY" if str_detect(x, "MRB=EY;") else (
                                                                "EYCN" if str_detect(x, "MRB=EYCN;") else (
                                                                "EYCY" if str_detect(x, "MRB=EYCY;") else (
                                                                "NN" if str_detect(x, "MRB=NN;") else "NA"))))))

    filtered_claims['CLAIM_INDICATOR_OTP'] = filtered_claims['CLAIM_INDICATOR'].apply(lambda x: "OTP=E" if str_detect(x, "OTP=E;") else (
                                                                "OTP=EN" if str_detect(x, "OTP=EN;") else (
                                                                "OTP=EY" if str_detect(x, "OTP=EY;") else (
                                                                "OTP=N" if str_detect(x, "OTP=N;") else "NA"))))

    filtered_claims['CLAIM_INDICATOR_WAF'] = filtered_claims['CLAIM_INDICATOR'].apply(lambda x: "WAF=Y" if str_detect(x, "WAF=Y;") else (
                                                                "WAF=N" if str_detect(x, "WAF=N;") else "NA"))


    print("Filtered and cleaned data size:")
    print(filtered_claims.shape)

    print("Counts of CCODEs present in dataset: ")
    print(filtered_claims.value_counts())


    print("removing dupes and dumping data")
    print("inititial size of ccode claims: ")
    print(filtered_claims.shape)

    # Remove duplicates by patient name, dos, dob, and provider tin
    filtered_claims = filtered_claims.sort_values(by=['PATIENT_FIRST_NM','PATIENT_LAST_NM',
                                            'PATIENT_BIRTH_DT','FIRST_DOS', 'ORIGINAL_PROVIDER_TAX_ID',
                                            'CLAIM_TYPE', 'PAYMENT_STATUS']) 
    filtered_claims = filtered_claims.drop_duplicates(['PATIENT_FIRST_NM', 'PATIENT_LAST_NM',
                                            'PATIENT_BIRTH_DT','FIRST_DOS', 'ORIGINAL_PROVIDER_TAX_ID',
                                            'CLAIM_TYPE'], keep="first") 
    print("after filtering for duplicates, size: ")
    print(filtered_claims.shape)

    if b_any('UHN' in x for x in list(filtered_claims['CLIENT_CD'].unique())):
        print("Removing excluded ccodes")
        excluded_ccodes = ['807-PROSOTHER', '807-PROSSNF', 'UHNEPPO', 'UHNFIUSPPOSTPAY', 'UHNLEEENT' ,'UHNXDIS', 'UHNPOSTPAY', 'UHNFI', 'UHNFIUSP', 'UHNFIMNRP'] 
        filtered_claims = filtered_claims[~filtered_claims['CLIENT_CD'].isin(excluded_ccodes)]
        print(filtered_claims.shape)
    else:
        FeeDescrip = pd.read_excel('/mnt/code/Benchmarking/SelfBill_Clients.xlsx')
        FeeDescrip.loc[FeeDescrip['PRODUCTCODE'] == 'DataISightPract', 'PRODUCTCODE'] = 'DIP'
        FeeDescrip.loc[FeeDescrip['PRODUCTCODE'] == 'DataISightFrac', 'PRODUCTCODE'] = 'DIS'
        filtered_claims.loc[filtered_claims['LAST_CLOSURE_NETWORK_CD'] == 'QBP','LAST_CLOSURE_NETWORK_CD'] = 'NSA_QPABasdPrcng'
        FeeDescrip = FeeDescrip.sort_values(by=['PRODUCTCODE', 'CCODE', 'FEETYPE']) 
        FeeDescrip = FeeDescrip.drop_duplicates(['PRODUCTCODE', 'CCODE'], keep="first") 

        mask = filtered_claims['LAST_CLOSURE_NETWORK_CD'].str.contains('Ext', case=False, na=False) & filtered_claims['PRODUCTCODE'].notnull() 
        filtered_claims.loc[mask, 'LAST_CLOSURE_NETWORK_CD'] = filtered_claims.loc[mask, 'PRODUCTCODE']

        print("Removing claims based on fee description for percent savings pricing ")
        filtered_claims = filtered_claims.merge(FeeDescrip[['CCODE', 'PRODUCTCODE']], how = 'inner', left_on = ['CLIENT_CD', 'LAST_CLOSURE_NETWORK_CD'], right_on = ['CCODE', 'PRODUCTCODE'])
        print(filtered_claims.shape)

    gt_file = output_path  + '.nodupes.gt_'+ str(charge_split) +'.filtered_cleaned.csv.bz2'
    lt_file = output_path  + '.nodupes.lt_'+ str(charge_split) + '.filtered_cleaned.csv.bz2'
    new_file = output_path  + '.nodupes.filtered_cleaned.csv.bz2'

    print("ccode file: " + new_file)
    filtered_claims.to_csv(new_file, index=False, sep='|' , compression='bz2', na_rep='NA')

def parse_date(date_str):
    try:
        return date_parser(date_str).strftime("%Y-%m-%d") 
    except (TypeError, ValueError):
        return pd.NaT

if __name__=="__main__":
    arg_parser =  argparse.ArgumentParser(description =' Process start and end dates.')
    arg_parser.add_argument('--start_date', help='Start date in YYYY-MM-DD format', required=True)
    arg_parser.add_argument('--end_date', help='End date in YYYY-MM-DD format', required=True)
    arg_parser.add_argument('--pull_date', help='Date of data dump in YYYY-MM-DD format', required=True)
    arg_parser.add_argument('--pcode', help='PCODE', required=True)
    
    args = arg_parser.parse_args()
   
    # Parse the dates & Parent Clietn Code
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    pull_date = parse_date(args.pull_date)
    pcode = args.pcode

    # Input file path
    raw_input_file_path =  f'/mnt/data/Fee_Recovery_2024/{pcode}/raw/{pcode}_merged_BNR_MPI_{start_date}_{end_date}_{pull_date}.csv.bz2'
  
  
    input_file_name = raw_input_file_path[raw_input_file_path.rindex('/')+1:]


    clean_raw_extract(raw_input_file_path)

