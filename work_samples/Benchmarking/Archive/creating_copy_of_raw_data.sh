#!/bin/bash

base_dir="/mnt/data/Fee_Recovery_2024"

# Loopt through each PCODE's raw directory
for pcode_dir in "$base_dir"/*/raw/; do
    # Find all the .csv.bz2 files in the raw directory
    for file in "$pcode_dir"*.csv.bz2; do
        # Extract the PCODE name from the directory path
        pcode_name=$(basename "$(dirname "$pcode_dir")")

        # Create the new file path without dates
        new_file="${pcode_dir}${pcode_name}_merged_BNR_MPI.csv.bz2"

        # Copy the file to the new name
        cp "$file" "$new_file"

        # Print a message to confirm the copy
        echo "Copied $file to $new_file"
    done
done