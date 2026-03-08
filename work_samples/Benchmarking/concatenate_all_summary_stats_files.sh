#!/bin/bash

# Base directories
base_dir="/mnt/data/Fee_Recovery_2024"

# final_dir="/mnt/data/Fee_Recovery_2024/Network_Summaries"
output_dir="/mnt/code/Benchmarking/"

# Define the output file path
# output_file="${output_dir}/summary_stats_by_ccode_monthly_2024-01-01_2024-12-31_2025-04-01.csv" # This is for summary stats
output_file="${output_dir}/time_to_payment_booking_by_ccode_monthly_2024-01-01_2024-12-31_2025-04-01.csv" # This is for booking curves
# Initialize a flag to check if the header is added
header_added=false

# Loop through all files in the base directory that match the pattern
# for csv_file in "$base_dir"/*/summary_stats_2/*_summary_stats_ccode_monthly_raw_2024-01-01_2024-12-31_2025-04-01.csv; do # This is for summary stats
for csv_file in "$base_dir"/*/booking_curve/*_booking_table_2024-01-01_2024-12-31_2025-04-01.csv; do # This is for booking curves
    # Check if the file exists
    if [ -f "$csv_file" ]; then
        # Extract the PCODE name from the directory structure
        pcode_name=$(basename "$(dirname "$(dirname "$csv_file")")")

        # If the header has not been added yet, add it with an extra "pcode" column
        if [ "$header_added" = false ]; then
            header=$(head -n 1 "$csv_file")
            echo "${header}, pcode" > "$output_file"
            header_added=true
        fi 
        # Append the content of the current csv file without the header and add the PCODE column
        tail -n +2 "$csv_file" | awk -v pcode="$pcode_name" '{print $0 "," pcode}' >> "$output_file"
    else
        echo "Warning: No files matching summary_stats_ccode_network were found."
    fi
done

echo "Concatenation completed. Output saved to $output_file"
