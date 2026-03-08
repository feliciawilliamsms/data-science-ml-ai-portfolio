#!/bin/bash

for i in NALC-P \
NHE-P \
NVN-P \
PAM-P \
PEH-P \
PHC-P \
PHLI-P \
PHPNI-P \
PMAM-P \
PMCS-P \
PSH-P \
PSHWC-P \
RHG-P \
RISMS-P \
RPG-P \
SEN-P \
SHPG-P \
SHPH-P \
SIS-P \
SMB-P \
SNF-P \
SSA-P \
STU-P \
TC3-P \
TCTGA-P \
TCTG-P \
THCPAM-P \
THCP-P \
TRUST-P;

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
