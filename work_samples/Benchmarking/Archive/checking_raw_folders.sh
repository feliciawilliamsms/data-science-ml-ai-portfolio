#!/bin/bash

# # Base directories
base_dir="/mnt/data/Fee_Recovery_2024"

# Loop through each PCODE's summary directory
for pcode_dir in "$base_dir"/*/; do
    # Construct the path to the raw directory
    raw_dir="${pcode_dir}/raw/"

    # Check if the directory exists
    if [ -d "$raw_dir" ]; then
        # Chekc if the raw directory is empty
        if [ -z "$(ls -A "$raw_dir")" ]; then
            echo "Raw directory exists but is empty: $raw_dir"
        else
            echo "Raw directory exists and is not empty: $raw_dir"
        fi 
    else
        echo "Raw directory is missing: $raw_dir"
    fi
done
    

