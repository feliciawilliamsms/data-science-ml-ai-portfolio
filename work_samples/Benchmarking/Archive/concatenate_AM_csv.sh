#!/bin/bash

# Define output file
output_file="/mnt/data/Fee_Recovery_2024/temp_unpaid_dumps_15Oct2024/all_AM_unpaid_claims.csv"


# Get the first csv file and copy the header to the output file
first_file=$(find /mnt/data/Fee_Recovery_2024/temp_unpaid_dumps_15Oct2024/ -type f -name "*.nodupes.filtered_cleaned.csv.bz2.unpaid.csv" | head -n 1)
head -n 1 "$first_file" > "$output_file"

# Loop through all the csv files and concatenate them, skipping the header except for the first one
find /mnt/data/Fee_Recovery_2024 -type f -name "*.nodupes.filtered_cleaned.csv.bz2.unpaid.csv" | while read file; do
    tail -n +2 "$file" >> "output_file"
done

echo "Concatenation completed. Output saved to $output_file"




