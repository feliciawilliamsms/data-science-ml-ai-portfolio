#!/bin/bash

for i in DEL-P \
EBMMT-P \
EKH-P \
ETFS-P \
EWRS-P \
FHI-P \
FOCASO-P \
GEMDB-P \
GLBA-P \
GND-P \
GOLDR-P \
GREAT-P \
GRH-P \
HAAL-P \
HAP-P \
HBCBS-P \
HCMA-P \
HCSC-P \
HHCG-P \
HLNK-P \
HMAI-P \
HMAPS-P \
HMASS-P \
HNP-P \
HPH-P \
HRR-P \
HSPC-P \
HSP-P \
IBX-P \
ICHP-P \
IMA-P \
IQAX-P \
ITE-P \
ITMT-P \
KBA-P \
KSP-P \
L711-P \
LST-P \
MAGAM-P \
MAGWC-P \
MAM-P \
MCC-P \
MEK-P \
MGM-P \
MMO-P;

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
