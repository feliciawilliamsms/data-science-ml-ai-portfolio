#!/bin/bash

# Output CSV file
output_csv="raw_files_listing.csv"

# Initialize the CSV file with a header
echo "Directory,File" > "$output_csv"

# Loop through the raw directories under each store
for raw_dir in "/mnt/data/Fee_Recovery_2024"/*/raw/; do
    # Loop through files in the current raw directory
    for file in "$raw_dir"; do 
            # Add the store name and file path to the CSV
            echo "$(basename "$raw_dir"),$file" >> "$output_csv"
    done

done

