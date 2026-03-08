#%%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
import seaborn as sns

file_path_decision_tree_leaf = '/mnt/data/Fee_Recovery_2024/BCBSMA-P/filtered_cleaned/BCBSMA-P_leaf_labeled.csv.bz2'
file_client_decision = '/mnt/data/Fee_Recovery_2024/CLAIM_ID BCBSMA_Client_Payment_Decision_20241023.csv'
# Read in data
df_client = pd.read_csv(file_client_decision)
df_leaf = pd.read_csv(file_path_decision_tree_leaf, compression='bz2', sep="|", encoding='utf-8')
print('----------------------------------------------------------------')
print('Number of Rows and Columns Client Payment Determination Data')
print('----------------------------------------------------------------')
print(df_client.shape)
print('Column Names')
print(df_client.columns)
print('----------------------------------------------------------------')
print('Number of Rows and Columns Full Data with Decision Tree Leaves')
print('----------------------------------------------------------------')
print(df_leaf.shape)
print('First Five Rows of Deceion Tree Leaf Data Frame')
print(df_leaf.head())

#%%
pd.set_option('display.max_columns', None)

#%%
df_paying = df_leaf[df_leaf['CLAIM_ID'].isin(df_client['CLAIM_ID  BCBSMA is paying'])]

print(df_paying.shape)

#%%
print(df_client['CLAIM_ID  BCBSMA is paying'].shape)

#%%
df_client[['CLAIM_ID  BCBSMA is paying']].head()

# client_determined_unpaid = df_client[['CLAIM_ID  BCBSMA is Not Paying']].rows.to_list()
df_not_paying = df_leaf[df_leaf['CLAIM_ID'].isin(df_client['CLAIM_ID BCBSMA is Not Paying'])]

print(df_not_paying.shape)
# print(df_client['CLAIM_ID  BCBSMA is Not Paying'].shape)

#%%
len(df_client['CLAIM_ID BCBSMA is Not Paying'])
# %%
df_client[0:5]
# %%

paid_leaves = df_paying[['leaf_label', 'CLAIM_ID']]
paid_leaves.sample(5)
#%%
paid_leaves['leaf_label'].value_counts()
#%%
unpaid_leaves = df_not_paying[['leaf_label', 'CLAIM_ID']]
unpaid_leaves['leaf_label'].value_counts()
#%%

