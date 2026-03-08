import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
import glob
from tkinter.filedialog import askdirectory
import sys
from pull_data import *
# import datetime
import matplotlib.pyplot as plt
from scipy.stats import norm
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import Lasso
from sklearn import preprocessing
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay,  recall_score, precision_score
from skopt.space.space import Integer, Real
from skopt import BayesSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from random import sample
from numpy.random import uniform
import numpy as np
np.random.seed(923)
from math import isnan
from sklearn.decomposition import PCA
#from kmodes.kprototypes import KPrototypes
import seaborn as sns
from sklearn.metrics import accuracy_score, confusion_matrix, roc_auc_score, roc_curve
from sklearn.tree import DecisionTreeClassifier # Import Decision Tree Classifier
import plotly.express as px
from datetime import date
import datetime

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




def create_tree(csv_path = '', pcode =''):
    pcodes_file = pd.read_excel('/mnt/pcodes_to_run.xlsx')
    pcodes_to_run = list(pcodes_file['Pcode'])

    for pcode in pcodes_to_run:
    #[ 'BCBSMN-P' , 'BCBSND-P' , 'BNWC-P' , 'CAPBC-P' , 'CBHS-P', 'CHPI-P' , 'CHPM-P' , 'FHI-P' ,'GRH-P', 
    #'HCSC-P' ,'IBX-P' ,'IQAX-P' , 'THCPAM-P' ,'UHNEIPI-P' , 'UHNPI-P' ,'UHNUSPEIPI-P' , 'WHS-P' , 'XRX-P']:

        print("reading data in chunks....")
        now = datetime.datetime.now()
        print(now.time())
        X_train_model_samp = pd.DataFrame()
        data_path = '/domino/datasets/Fee_Recovery_2024/' + pcode + '/filtered_cleaned/'
        file_name = find_file_with_ending(data_path, 'nodupes.filtered_cleaned.csv.factors.csv.bz2')
        if file_name == None:
            continue
        csv_path = data_path + '/' + file_name
        for chunk in pd.read_csv(csv_path, delimiter = '|', compression='bz2', chunksize=15000):
            X_train_model_samp = pd.concat([X_train_model_samp, chunk], ignore_index=True) #
        print("data read....")
        now = datetime.datetime.now()
        print(now.time())

        train_target = X_train_model_samp['target']
        #X_train_model_samp = X_train_model_samp.drop('target', axis=1)

        # Create Decision Tree classifer object
        clf = DecisionTreeClassifier(criterion="entropy", max_depth=5)

        """
        X_train_model['target'] = train_target
        X_train_model_samp = X_train_model
        train_target = X_train_model_samp['target']
        X_train_model_samp = X_train_model_samp.drop('target', axis=1)
        """

        print("training first....")
        now = datetime.datetime.now()
        print(now.time())
        # Train Decision Tree Classifer
        try:
            clf = clf.fit(X_train_model_samp.drop(['CLAIM_ID','target'], axis=1),train_target)
            columns_dt = list(X_train_model_samp.drop([ 'CLAIM_ID', 'target'], axis=1).columns)
            
        except:
            clf = clf.fit(X_train_model_samp.drop(['PAYMENT_STATUS_UNPAID', 'CLAIM_ID','leaf_label'], axis=1),train_target)
            columns_dt = list(X_train_model_samp.drop(['PAYMENT_STATUS_UNPAID', 'CLAIM_ID', 'leaf_label'], axis=1).columns)

        print("training done....")
        now = datetime.datetime.now()
        print(now.time())
        # Creates decision tree png

        from six import StringIO 
        from IPython.display import Image  
        from sklearn.tree import export_graphviz
        import pydotplus
        dot_data = StringIO()
        export_graphviz(clf, out_file=dot_data,  
                        filled=True, rounded=True,
                        special_characters=True, feature_names = columns_dt,class_names=['0','1'], node_ids=True)
        graph = pydotplus.graph_from_dot_data(dot_data.getvalue())  
        graph.write_png(pcode + '_tree_8_21.png')
        Image(graph.create_png())

        """
        # Get the features used for splits
        used_features = clf.tree_.feature

        # Filter out -2 which is the default for leaf nodes and get unique feature indices
        split_feature_indices = np.unique(used_features[used_features >= 0])

        # Get the feature names for the used features
        split_feature_names = [columns_dt[i] for i in split_feature_indices] 

        # Drop unused columns
        X_df_reduced = X_train_model_samp[split_feature_names + ['CLAIM_ID','target']]


        print("retraining started....")
        now = datetime.datetime.now()
        print(now.time())
        # Retrain the DecisionTreeClassifier with only the used features
        clf_retrained = DecisionTreeClassifier()
        clf_retrained.fit(X_df_reduced.drop(['CLAIM_ID','target'], axis=1), train_target)
        from joblib import dump, load
        dump(clf_retrained, 'DT_model_ANTFIOTHR.joblib')
        X_df_reduced.to_csv('/domino/datasets/Fee_Recovery_2024/' + pcode + '/filtered_cleaned/factors_reduced_ANTFIOTHR.csv.bz2', compression='bz2', sep='|', index=False)
        
        print("retraining done....")
        now = datetime.datetime.now()
        print(now.time())

        dot_data2 = StringIO()
        export_graphviz(clf_retrained, out_file=dot_data2,  
                        filled=True, rounded=True,
                        special_characters=True, feature_names = split_feature_names,class_names=['0','1'], node_ids=True)
        graph2 = pydotplus.graph_from_dot_data(dot_data2.getvalue())  
        graph2.write_png('' + pcode + '_tree_7_30.png')
        Image(graph2.create_png())
        """

        from sklearn import tree
        
        # Select columns with 'float64' dtype  
        float64_cols = list(X_train_model_samp.select_dtypes(include='float64'))
        int64_cols = list(X_train_model_samp.select_dtypes(include='int64'))
        int64_cols.remove('CLAIM_ID')
        #int64_cols - ['CLAIM_ID', 'leaf_label']

        temp = X_train_model_samp[float64_cols + int64_cols].astype(np.float32)
        X_train_model_samp = X_train_model_samp.drop(columns=float64_cols+int64_cols)
        X_train_model_samp[float64_cols+int64_cols] = temp


        """
        # The same code again calling the columns
        for col in float64_cols:
            X_train_model_samp[col] = np.array(X_train_model_samp[col], dtype=np.float32)
            print(col)
            #X_train_model_samp[float64_cols] = X_train_model_samp[float64_cols].astype('float32')
        for col in int64_cols:
            X_train_model_samp[col] = np.array(X_train_model_samp[col], dtype=np.float32)    
            print(col)
            #X_train_model_samp[int64_cols] = X_train_model_samp[int64_cols].astype('float32')
        
        """

        # Now I want to know the corresponding leaf node id for each of my training data point
        leaf_dict = {}
        num_parts = 100
        df_parts = np.array_split(X_train_model_samp, num_parts)

        print("start labeling....")
        now = datetime.datetime.now()
        print(now.time())

        for i, part in enumerate(df_parts):
            try:
                part['leaf_label'] = clf.tree_.apply(np.array(part.drop(['PAYMENT_STATUS_UNPAID', 'CLAIM_ID','leaf_label','MultiPlan Allowable Used? (YES, NO or Could Not Find)'], axis=1)))
            except:    
                part['leaf_label'] = clf.tree_.apply(np.array(part.drop(['CLAIM_ID','target'], axis=1)))
            if i == 0:
                part.to_csv('/domino/datasets/Fee_Recovery_2024/' + pcode + '/filtered_cleaned/' + pcode + '_leaf_labeled.csv.bz2', compression='bz2', sep='|', index=False)
            else:
                part.to_csv('/domino/datasets/Fee_Recovery_2024/' + pcode + '/filtered_cleaned/' + pcode + '_leaf_labeled.csv.bz2', compression='bz2', sep='|', mode='a', header=False, index=False)
            print("part " + str(i) + " appended")
        
        #X_train_model_samp.to_csv('/domino/datasets/Fee_Recovery_2024/' + pcode + '/filtered_cleaned/' + pcode + '_leaf_labeled.csv.bz2', compression='bz2', sep='|')

        




def get_benchmarks(run_clustering=False):
    train_data = pd.read_pickle('/domino/datasets/Fee_Recovery_2024/UHN-P/UHN-P_train.pkl')


def get_sample_claims(leafs = [1,2,3,4]):

    # Read in dataset with leaves labeled
    claim_samples = pd.read_csv('/domino/datasets/Fee_Recovery_2024/' + pcode + '/filtered_cleaned/' + pcode + '_leaf_labeled.csv.bz2', sep='|', compression='bz2') #+ str(date.today()).replace(" ", "") + '.pkl')
    #sent_claims = pd.read_excel('/mnt/ML-client-fee-recovery/AllClaimsSentThrough606.xlsx')
    train_data = pd.read_csv('/domino/datasets/Fee_Recovery_2024/' + pcode + '/raw/' + pcode + '_merged_BNR_MPI_2023-01-01_2024-12-01_2024-07-25.csv.bz2', sep='|', compression='bz2')
    sent_ids = [] #list(sent_claims['CLAIM_ID'])


    # Filter leaves we want to send
    claim_samples = claim_samples[(claim_samples['leaf_label'].isin(leafs)) & ~(claim_samples['CLAIM_ID'].isin(sent_ids))] #& (claim_samples['MAGIC_SOLUTION_CLASS'].isin(['Network Management Services','Primary Network', 'Extender Networks', 'Complementary Non-Logo Network', 'Complementary Logo Network']))]
    train_data = train_data[train_data['CLAIM_ID'].isin(list(claim_samples['CLAIM_ID']))]
    claim_samples = train_data.merge(claim_samples[['CLAIM_ID', 'leaf_label']], how='right', on='CLAIM_ID')

    claim_samples['RECEIVED_DT'] = pd.to_datetime(claim_samples['RECEIVED_DT'])
    claim_samples['RECEIVED_YR'], claim_samples['RECEIVED_MT'] = claim_samples['RECEIVED_DT'].dt.year, claim_samples['RECEIVED_DT'].dt.month
    claim_samples = claim_samples[ (claim_samples['RECEIVED_YR'] == 2024) & (claim_samples['RECEIVED_MT'].isin([1,2,3,4,5])) ]
    claim_samples = claim_samples[(claim_samples['PAYMENT_STATUS'] == 'UNPAID')]

    columns_keeps = ['CLAIM_ID',	'CLAIM_TYPE','PROVIDERGROUPNAME' ,	'CLAIM_SPECIALTY',	'CLIENT_CD',	'TOTAL_CHARGE_AMT',	'CPT',	'CLAIM_FAIL_REASON_CD',	'ENGINE',	'FUNDING_TYPE',	'CITY_NM',	'CLIENT_NM',	'PROVIDER_COUNTY_NAME',	'PROCESSED_DT',	'STATE_CD',	'ZIP3',	'LAST_UPDATED_TS',	'MAGIC_SOLUTION_CLASS',	'MBM_VALID_CHARGES', 'SAVINGS_AMT',	'NETWORK_CD',	'POS_DESCRIPTION_HCE', 'PATIENT_FIRST_NM', 'PATIENT_LAST_NM','RENDERING_PROVIDER_TIN','leaf_label']
    
    claim_samples[columns_keeps].sort_values([ 'SAVINGS_AMT'], ascending=False).to_csv('ANTFIOTHR_8_06_l19.csv')


    feedback = pd.read_excel('/mnt/sample_claims_UHNEMP_DB_limited 5_9.xlsx')
    claim_repo = pd.read_excel('/mnt/ClaimRepo.xlsx')

    claims_repo = claim_repo.append(claim_samples)

    claims_out = pd.merge(claims_repo, feedback, left_on='CLAIM_ID', right_on='RIMS_CLAIM_NUMBER',how='left')


    




if __name__ == "__main__":
    try:
        #full_data_path = sys.argv[0]
        factor_path = sys.argv[0]
        split_data = sys.argv[1]

    except:
        split_data = False
        #factor_path = '/domino/datasets/Fee_Recovery_2024/' + pcode + '/filtered_cleaned/' + pcode + '_merged_BNR_MPI_2023-01-01_2024-04-30_2024-08-13.nodupes.filtered_cleaned.csv.factors.csv.bz2'

    #if split_data:
        #full_data = pd.read_pickle('/domino/datasets/Fee_Recovery_2024/MEDIFB-P/filtered_cleaned/MEDIFB-P_merged_BNR_MPI_2023-01-01 00:00:00_2024-12-01 00:00:00_2024-07-10.nodupes.filtered_cleaned.csv.factors.csv.bz2')
        #split_pcode(full_data)

    #mod_files()
    create_tree()
    #get_sample_claims(list([20,22,23,28])) #5,8,9,16,34

    #model_data(run_clustering=False)

    
    
    a=1
