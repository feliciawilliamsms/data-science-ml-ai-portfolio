#!/bin/bash

for i in AAE-P \
ABN-P \
ACSC-P \
ALNA-P \
ANFINY-P \
ANTFIOTHR-P \
ARP-P \
ASF-P \
ASOL-P \
ASON-P \
ASRM-P \
AUS-P \
AUTIS-P \
AVB-P \
AWC-P \
BCBSMA-P \
BCBSMI-P \
BCBSMN-P \
BCBSND-P \
BNWC-P \
BROKE-P \
BSA-P \
CAHI-P \
CAPBC-P \
CBH-P \
CBHS-P \
CBSA-P \
CCIH-P \
CETV-P \
CHPI-P \
CHPM-P \
CIGSAR-P \
COF-P \
COMPIQ-P \
COP-P \
CR-P \
CSG-P \
CSGW-P \
CSH-P \
CVAL-P \
CVCAM-P \
CVC-P \
CWCP-P;



do 
    # pull_date=${pull_date:-$(date +%Y-%m-%d)} # Set to current date if not provided
    pull_date=2025-04-01 # Set to current date if not provided
    start_date=2024-01-01
    end_date=2024-12-31

    # python Benchmarking/data_pull_sql_query.py --start_date "$start_date" --end_date "$end_date" --pull_date "$pull_date" --pcode "$i"; # Add a pull date that is equal to the temp_date
    # python Benchmarking/filtering_cleaning.py --start_date "$start_date" --end_date "$end_date" --pull_date "$pull_date" --pcode "$i";
    # python unpaid_claims_for_Acct_Mngrs.py --start_date "$start_date" --end_date "$end_date" --pull_date "$pull_date" --pcode "$i";
    # python booking_table_FILE.py --start_date "$start_date" --end_date "$end_date" --pull_date "$pull_date" --pcode "$i";
    python summary_stats_by_pcode_by_month.py --start_date "$start_date" --end_date "$end_date" --pull_date "$pull_date" --pcode "$i";

done
