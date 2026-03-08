#!/bin/bash

# for i in AAE-P \
# ABN-P \
# ACSC-P \
# ALNA-P \
# ANFINY-P \
# ANTFIOTHR-P \
# ARP-P \
# ASF-P \
# ASOL-P \
# ASON-P \
# ASRM-P \
# AUS-P \
# AUTIS-P \
# AVB-P \
# AWC-P \
# BCBSMA-P \
# BCBSMI-P \
# BCBSMN-P \
# BCBSND-P \
# BNWC-P \
# BROKE-P \
# BSA-P \
# CAHI-P \
# CAPBC-P \
# CBH-P \
# CBHS-P \
# CBSA-P \
# CCIH-P \
# CETV-P \
# CHPI-P \
# CHPM-P \
# CIGSAR-P \
# COF-P \
# COMPIQ-P \
# COP-P \
# CR-P \
# CSG-P \
# CSGW-P \
# CSH-P \
# CVAL-P \
# CVCAM-P \
# CVC-P \
# CWCP-P \
# DEL-P \
# EBMMT-P \
# EKH-P \
# ETFS-P \
# EWRS-P \
# FHI-P \
# FOCASO-P \
# GEMDB-P \
# GLBA-P \
# GND-P \
# GOLDR-P \
# GREAT-P \
# GRH-P \
# HAAL-P \
# HAP-P \
# HBCBS-P \
# HCMA-P \
# HCSC-P \
# HHCG-P \
# HLNK-P \
# HMAI-P \
# HMAPS-P \
# HMASS-P \
# HNP-P \
# HPH-P \
# HRR-P \
# HSPC-P \
# HSP-P \
# IBX-P \
# ICHP-P \
# IMA-P \
# IQAX-P \
# ITE-P \
# ITMT-P \
# KBA-P \
# KSP-P \
# L711-P \
# LST-P \
# MAGAM-P \
# MAGWC-P \
# MAM-P \
# MCC-P \
# MEK-P \
# MGM-P \
# MMO-P \
# NALC-P \
# NHE-P \
# NVN-P \
# PAM-P \
# PEH-P \
# PHC-P \
# PHLI-P \
# PHPNI-P \
# PMAM-P \
# PMCS-P \
# PSH-P \
# PSHWC-P \
# RHG-P \
# RISMS-P \
# RPG-P \
# SEN-P \
# SHPG-P \
# SHPH-P \
# SIS-P \
# SMB-P \
# SNF-P \
# SSA-P \
# STU-P \
# TC3-P \
# TCTGA-P \
# TCTG-P \
# THCPAM-P \
# THCP-P \
# TRUST-P \
# UHN-P \
# UHNEIPI-P \
# UHNPI-P \
# UHNUSPEIPI-P \
# UPMC-P \
# USNAS-P \
# VNT-P \
# VPG-P \
# WAU-P \
# WHS-P \
# XFCHP-P \
# XOXF-P \
# XRX-P;
for i in  AAE-P;
do

# for i in SEN-P;
# do 
    # pull_date=${pull_date:-$(date +%Y-%m-%d)} # Set to current date if not provided
    pull_date=2025-05-05 # Set to current date if not provided
    start_date=2024-01-01
    end_date=2024-12-31

    # python data_pull_sql_query.py --start_date "$start_date" --end_date "$end_date" --pull_date "$pull_date" --pcode "$i"; # Add a pull date that is equal to the temp_date
    python filtering_cleaning.py --start_date "$start_date" --end_date "$end_date" --pull_date "$pull_date" --pcode "$i";
    # python unpaid_claims_for_Acct_Mngrs.py --start_date "$start_date" --end_date "$end_date" --pull_date "$pull_date" --pcode "$i";
    # python booking_table_FILE.py --start_date "$start_date" --end_date "$end_date" --pull_date "$pull_date" --pcode "$i";
    python summary_stats_by_pcode_by_month.py --start_date "$start_date" --end_date "$end_date" --pull_date "$pull_date" --pcode "$i";

done
