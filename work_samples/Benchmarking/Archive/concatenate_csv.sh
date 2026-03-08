#!/bin/bash

# Define output file
output_file="/mnt/data/Fee_Recovery_2024/Network_Stats/all_ccode_by_network_summary_stats_updated.csv"


# Get the first csv file and copy the header to the output file
first_file=$(find /mnt/data/Fee_Recovery_2024/*/ -type f -name "*_summary_stats_ccode_network*.csv" | head -n 1)
head -n 1 "$first_file" > "$output_file"

# Loop through all the csv files and concatenate them, skipping the header except for the first one
find /mnt/data/Fee_Recovery_2024 -type f -name "*summary_stats_ccode_network*.csv" | while read file; do
    tail -n +2 "$file" >> "output_file"
done

echo "Concatenation completed. Output saved to $output_file"