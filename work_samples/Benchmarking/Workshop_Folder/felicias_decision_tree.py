# #%%
# # import packages 
# import numpy as np 
# import pandas as pd 
# import matplotlib.pyplot as plt



# # Set maximum number of viewable rows and columns  
# pd.set_option('display.max_rows', 800)
# pd.set_option('display.max_columns', 60)

# # file location
# file_path = "/mnt/data/Fee_Recovery_2024/BCBSMA-P/filtered_cleaned/BCBSMA-P_merged_BNR_MPI_2023-01-01_2024-04-30_2024-08-13.nodupes.filtered_cleaned.csv.factors.csv.bz2"

# # Read in data
# df = pd.read_csv(file_path, compression='bz2', sep="|", encoding='utf-8')
# df.head()
# #%%
# df.shape
# # %%
# # # Prepare the data 
# # Creating input and output feature dataframes
# y = df[['target']]
# # Drop claim ID and target columns 
# X = df.drop(['CLAIM_ID','target'], axis=1)


# from sklearn. model_selection import train_test_split
# X_train, X_test, y_train, y_test = train_test_split(X, y, 
# train_size = 0.8, stratify = y, random_state=12345)

# # X_train.shape, X_test.shape

# # Train and Evaluate the Classification tree
# from sklearn.tree import DecisionTreeClassifier
# classifier =  DecisionTreeClassifier(max_depth=5, criterion="entropy", random_state=12345)

# model = classifier.fit(X_train, y_train)
# # model.score(X_test, y_test)
# # %%
# # # Visualize the Classification Tree
# from sklearn import tree
# final_model = classifier.fit(X,y)
# fig = plt.figure(figsize = (60,15), dpi=300)
# tree.plot_tree(final_model, feature_names = list(X.columns),
# class_names = ['0','1'], #class_names = ['Unpaid', 'Paid'],
# filled = True,
# rounded=True,
# fontsize=8,
# proportion=False,
# node_ids=True)
# plt.show()
# # Save the figure as a png file
# fig.savefig("BCBSMA-P_decision_tree.png")

#%%
# from sklearn.metrics import classification_report, plot_confusion_matrix
# print(classification_report(y_test, model.predict(X_test)))

# plot_confusion_matrix(model, X_test, y_test);

# #%%
# pd.DataFrame(index=X.columns, data=model.feature_importances_, columns=['Feature Importance']).sort_values(by='Feature Importance', ascending=False)
# # %%

#%%
import os
import pandas as pd
import matplotlib.pyplot as plt 
from sklearn.tree import DecisionTreeClassifier
from sklearn import tree

# Load pcodes and date extensions from CSV file
pcodes_df = pd.read_csv('/mnt/data/Fee_Recovery_2024/temp/remaining_trees/pcodes.csv')
dates_df = pd.read_csv('/mnt/data/Fee_Recovery_2024/temp/remaining_trees/date_extensions.csv')

# Extract pcode and date extensions to a list from the dataframes
pcodes = pcodes_df['pcode'].tolist()
date_extensions = dates_df['date_extension'].to_list()

# # Base file path template
base_path = "/mnt/data/Fee_Recovery_2024/{pcode}/filtered_cleaned/{pcode}_merged_BNR_MPI_{date_extension}.nodupes.filtered_cleaned.csv.factors.csv.bz2"

# Function to load data, fit a decision tree, and save the plot
def generate_decision_tree(pcode, date_extensions):
    file_found = False

    # Loop through each date extension
    for date_extension in date_extensions:
        # Construct the file path
        file_path = base_path.format(pcode=pcode, date_extension=date_extension)

        # Check if the file exists
        if os.path.exists(file_path):
            print(f"Processing file: {file_path}")

            # Load the data
            df = pd.read_csv(file_path, compression='bz2', sep="|", encoding='utf-8')

            # Prepare X (features) and y (target)
            y = df[['target']] 
            X = df.drop(['CLAIM_ID','target'], axis=1) # Drop claim ID and target columns 

            # Fit the decision tree
            classifier =  DecisionTreeClassifier(max_depth=5, criterion="entropy", random_state=12345)
            clf = classifier.fit(X,y)

            # Create the plot
            fig = plt.figure(figsize = (60,15), dpi=300)
            tree.plot_tree(clf, 
            feature_names = list(X.columns),
            class_names = ['0','1'], #class_names = ['Unpaid', 'Paid'],
            filled = True,
            rounded=True,
            fontsize=8,
            proportion=False,
            node_ids=True)

            # show the plot
            # plt.show()

            # Save the figure
            output_file_path = f"/mnt/data/Fee_Recovery_2024/temp/remaining_trees/{pcode}_decision_tree.png"
            fig.savefig(output_file_path)
            print(f"Decision tree plot saved as {output_file_path}")

            # Clear the plot after saving
            plt.clf()

            # Set the flag to indicate a file was found
            file_found = True
            break  # Stop after finding the first existing file for the PCODE.
  
    if not file_found:
        print(f"No file found for {pcode} with the given date extensions.")
# Loop over each pcode and date extension
for pcode in pcodes:
    generate_decision_tree(pcode, date_extensions)




#%%
####################################Fast AdHoc Decision Tree Runs #######################################
########################################This is GOOD Script!#############################################
import os
import pandas as pd
import matplotlib.pyplot as plt 
from sklearn.tree import DecisionTreeClassifier
from sklearn import tree
from scipy.sparse import csr_matrix # new line to test out

# Load the data
# Function to load data, fit a decision tree, and save the plot

def generate_decision_tree(file_path):
    """ Generates decison trees for list of clients. 
        Produces a dataframe with the leaf assignment and probabilities.
        Produces decision tree graphic with leaf node numbers.
    """
# Load the data
    df = pd.read_csv(file_path, compression='bz2', sep="|", encoding='utf-8')

    # Extract Parent_client code from the file path
    # pcode =  os.path.basename(file_path).split('_')[0]

     # Convert to date time fields
    df['PROCESSED_DT'] = pd.to_datetime(df['PROCESSED_DT'], errors='coerce')
    
    # Choose date range if needed could create parameters here
    df = df[(df['PROCESSED_DT'] > '2024-04-30') & (df['PROCESSED_DT'] < '2024-11-01')]
    
    # Subset the data to the required columns or factors

    df = df[['CLAIM_INDICATOR',
            'CLAIM_TYPE',
            'CLAIM_SPECIALTY',
            'CLIENT_CD',
            'TOTAL_CHARGE_AMT',
            'CPT',
            'STATE_CD',
            'ZIP5',
            'NON_COVERED_CHARGE_AMT',
            'SAVINGS_AMT',
            'TYPEOFBILL',
            'CLIENT_SURPRISE_BILL_IND',
            'ADJUSTMENTSTATUS',
            'STATUS_CODE',
            'CLAIMSTATUS',
            'LAST_CLOSURE_NETWORK_CD',
            'PAYMENT_STATUS'
            ]]
   
    # Convert all character features to string data type
    # List of columns to convert to character type
    character_columns = ['CLAIM_INDICATOR',
                        'CLAIM_TYPE',
                        'CLAIM_SPECIALTY',
                        'CLIENT_CD',
                        'CPT',
                        'STATE_CD',
                        'ZIP5',
                        'TYPEOFBILL',
                        'CLIENT_SURPRISE_BILL_IND',
                        'ADJUSTMENTSTATUS',
                        'STATUS_CODE',
                        'CLAIMSTATUS',
                        'LAST_CLOSURE_NETWORK_CD'
                        ]

    # Convert columns to character type and lowercase for standardization
    for column in character_columns:
        df[column] = df[column].astype('category')
    
    # Convert character columns to lowercase
    df[column] = df[column].apply(lambda x: x.lower())
    
    # Keep on the first three digits of the Zip Code (Zip 3)
    df['ZIP3'] = df['ZIP5'].astype(str).str[:3]

    # Convert all numeric features to float data type    
    # List of columns to convert to float type
    numeric_columns = [ 'TOTAL_CHARGE_AMT', 'NON_COVERED_CHARGE_AMT', 'SAVINGS_AMT']

    # Convert columns to numeric type
    for column in numeric_columns:
        df[column] = df[column].astype(float)
    
    # Prepare X (features) and y (target)
    y = df['PAYMENT_STATUS']
    X = df.drop(columns=['PAYMENT_STATUS', 'ZIP5']) # Drop claim ID and target columns    
    
    # Convert categorical variables to numerical 
    X = pd.get_dummies(X, sparse=True)
    
    # Ensure all numeric values
    X = X.astype(float)

    # Fit the decision tree
    classifier =  DecisionTreeClassifier(max_depth=5, criterion="entropy", random_state=12345)
    clf = classifier.fit(X,y)
    
    # Feature Importance Check
    feature_importances = clf.feature_importances_
    important_features = {feat: imp for feat, imp in zip(X.columns, feature_importances) if imp > 0}
    print("Important Features:", important_features)

    # Assign leaf nodes to DataFrame
    leaf_nodes = clf.apply(X)
    df['leaf_node'] = leaf_nodes

    # Add predicted probabilities to DataFrame
    probas = clf.predict_proba(X)
    df[['prob_paid', 'prob_unpaid']] = probas
    # Plot the decision tree
    # fig = plt.figure(figsize = (60,15), dpi=300)
    fig, ax = plt.subplots(figsize=(30,10), dpi=300)
    tree.plot_tree(clf, 
    feature_names = X.columns,
    class_names = ['Paid', 'Unpaid'], # class_names = ['0','1']
    filled = True,
    rounded=True,
    fontsize=8,
    proportion=False)
    
    # Annotate leaf nodes
    for idx, text in enumerate(ax.texts):
        if 'class =' in text.get_text():
            text.set_text(f"{text.get_text()}\nLeaf: {idx}")

    # Add spacing between the nodes
    ax.margins(x=0.2, y=0.2) # Increases space around the nodes for visibility
    # Show the figure
    plt.show()

    # Save the figure
    output_file_path = f"/mnt/code/Benchmarking/tree_{p}_decision_tree_2024-05-01_2024-10-31_dump_2025-01-09.png"
    fig.savefig(output_file_path)
    print(f"Decision tree plot saved as {output_file_path}")

    return df
pcode_list = [  'BCBSMA-P'
                # 'WAU-P',
                # 'CBSA-P',
                # 'IMA-P',
                # 'UHN-P',
                # 'LST-P',
                # 'HBCBS-P',
                # 'HCSC-P',
                # 'HSP-P',
                # 'CWCP-P',
                # 'AUS-P',
                # 'GREAT-P',
                # 'ASON-P',
                # 'ASOL-P',
                # 'AAE-P',
                # 'CHPI-P',
                # 'PSHWC-P'
                ]
for p in pcode_list:
    file_path = f'/mnt/data/Fee_Recovery_2024/{p}/filtered_cleaned/{p}_merged_BNR_MPI_2024-01-01_2024-12-31_2025-01-09.nodupes.filtered_cleaned.csv.bz2'

result_df = generate_decision_tree(file_path)
print(result_df.head())

################################################################################################################################################################################
# %%
# import pandas as pd
# p = 'WAU-P'
# file_path = f'/mnt/data/Fee_Recovery_2024/{p}/filtered_cleaned/{p}_merged_BNR_MPI_2024-01-01_2024-12-31_2025-01-09.nodupes.filtered_cleaned.csv.bz2'
# df = pd.read_csv(file_path, compression='bz2', sep="|", encoding='utf-8')
# df['PROCESSED_DT'] = pd.to_datetime(df['PROCESSED_DT'], errors='coerce')
    
# # Choose date range if needed could create parameters here
# df = df[(df['PROCESSED_DT'] > '2024-04-30') & (df['PROCESSED_DT'] < '2024-11-01')]
    
# # Subset the data to the required columns or factors
# # Removed Product Code. Does not have enough levels. It is either dataisightpract or 0 or null.
# df = df[['CLAIM_INDICATOR',
#             'CLAIM_TYPE',
#             'CLAIM_SPECIALTY',
#             'CLIENT_CD',
#             'TOTAL_CHARGE_AMT',
#             'CPT',
#             'STATE_CD',
#             'ZIP5',
#             'NON_COVERED_CHARGE_AMT',
#             'SAVINGS_AMT',
#             'TYPEOFBILL',
#             'CLIENT_SURPRISE_BILL_IND',
#             'ADJUSTMENTSTATUS',
#             'CLAIMSTATUS',
#             'PAYMENT_STATUS',
#             'POS_DESCRIPTION_HCE',
#             'LAST_CLOSURE_NETWORK_CD'
#         ]]
   
# # Convert all character features to string data type
# # List of columns to convert to character type
# character_columns = ['CLAIM_INDICATOR',
#                         'CLAIM_TYPE',
#                         'CLAIM_SPECIALTY',
#                         'CLIENT_CD',
#                         'CPT',
#                         'STATE_CD',
#                         'ZIP5',
#                         'TYPEOFBILL',
#                         'CLIENT_SURPRISE_BILL_IND',
#                         'ADJUSTMENTSTATUS',
#                         'CLAIMSTATUS',
#                         'LAST_CLOSURE_NETWORK_CD',
#                         'POS_DESCRIPTION_HCE'
#                         ]

# # Convert columns to character type and lowercase for standardization
# for column in character_columns:
#     df[column] = df[column].astype(str)
#     # Convert character columns to lowercase
#     df[column] = df[column].apply(lambda x: x.lower())
    
# # Keep on the first three digits of the Zip Code (Zip 3)
# df['ZIP3'] = df['ZIP5'].astype(str).str[:3]

# # Convert all numeric features to float data type    
# # List of columns to convert to float type
# numeric_columns = ['TOTAL_CHARGE_AMT', 'NON_COVERED_CHARGE_AMT', 'SAVINGS_AMT']

# # Convert columns to character type
# for column in numeric_columns:
#     df[column] = df[column].astype(float)
# # %%
# ['CLAIM_INDICATOR',
#                         'CLAIM_TYPE',
#                         'CLAIM_SPECIALTY',
#                         'CLIENT_CD',
#                         'CPT',
#                         'STATE_CD',
#                         'ZIP5',
#                         'TYPEOFBILL',
#                         'PRODUCTCODE_x',
#                         'CLIENT_SURPRISE_BILL_IND',
#                         'ADJUSTMENTSTATUS',
#                         'CLAIMSTATUS',
#                         'LAST_CLOSURE_NETWORK_CD',
#                         'POS_DESCRIPTION_HCE'
#                         ]
# #%%
# # # Set maximum number of viewable rows and columns  
# pd.set_option('display.max_rows', 800)
# pd.set_option('display.max_columns', 60)
# df['PAYMENT_STATUS'].value_counts()
# # %%
# # Load libraries
# import pandas as pd

# # Specify PCODE
# p = 'HBCBS-P'
# # Specify file path
# file_path = f'/mnt/data/Fee_Recovery_2024/{p}/filtered_cleaned/{p}_merged_BNR_MPI_2024-01-01_2024-12-31_2025-01-09.nodupes.filtered_cleaned.csv.bz2'

# # Read in thea dataframe
# df = pd.read_csv(file_path, compression='bz2', sep="|", encoding='utf-8')

# # Format processed date as a date field
# df['PROCESSED_DT'] = pd.to_datetime(df['PROCESSED_DT'], errors='coerce')
    
# # Specify data date range 
# df = df[(df['PROCESSED_DT'] > '2024-04-30') & (df['PROCESSED_DT'] < '2024-11-01')]

# # Keep on the first three digits of the Zip Code (Zip 3)
# df['ZIP3'] = df['ZIP5'].astype(str).str[:3]

# df.shape 
# #%%
# df['CLIENT_SURPRISE_BILL_IND'].value_counts()
# #%%  
# # Packet Filtering
# df_filtered = df[(df['TYPEOFBILL']== 'O') & (df['PAYMENT_STATUS']=='UNPAID')]





# # Select final dataframe columns
# df_final = df_filtered[['CLIENT_CD',
# 'CLAIM_ID',
# 'SUBMITTERCLAIMNUMBER',
# 'CLIENT_ADJUSTMENT_CLAIM_NUMBER',
# 'FIRST_DOS',
# 'RECEIVED_DT',
# 'MEMBER_ID',
# 'PATIENT_FIRST_NM',
# 'PATIENT_LAST_NM',
# 'PATIENT_BIRTH_DT',
# 'SOURCE_CD',
# 'TOTAL_CHARGE_AMT',
# 'SAVINGS_AMT',
# 'ALLOWED_CHARGE_AMT',
# 'APPEALED_FLAG',
# 'CLAIM_INDICATOR',
# 'CLAIM_SPECIALTY',
# 'CLAIM_TYPE',
# 'CONTRACT_TYPE',
# 'LAST_CLOSURE_NETWORK_CD',
# 'MBM_VALID_CHARGES',
# 'MPI_SURPRISE_BILL_IND',
# 'PARENTCLIENTCODE',
# 'POS_DESCRIPTION_HCE',
# 'PROVIDER_STATE',
# 'PROVIDER_STATE_RENDERING',
# 'PROVIDER_ZIP_CODE',
# 'PROVIDERGROUPNAME',
# 'REASONABLE_AND_CUSTOMARY_AMT',
# 'RENDERING_PROVIDER_TIN',
# 'ORIGINAL_PROVIDER_TAX_ID',
# 'PROVIDER_NPI',
# 'ZIP3',
# 'PAYMENT_STATUS']]


# df_final.shape
# #%%

# # Output file path
# output_file_path = f"/mnt/code/Benchmarking/Packet_{p}_merged_BNR_MPI_2024-05-01_2024-10-31_2025-01-09.nodupes.filtered_cleaned.csv.bz2.unpaid.csv"

# # Export file as CSV.bz2 compressed file
# df_final.to_csv(output_file_path, index=False)

# # %%
# df['STATE_CD'].value_counts()
# # %%
