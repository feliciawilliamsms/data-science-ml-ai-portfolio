#!/bin/bash

# Base directory
base_dir="/mnt/data/Fee_Recovery_2024"

# Loop through filtered_cleaned directories for each PCDOE
for csv_file in "$base_dir"/*/filtered_cleaned/*.nodupes.filtered_cleaned.csv.bz2; do 
    # Skip files that contain '.factirs' in the name
    if [[ "$csv_file" == *.factors.* ]];  then 
        continue
    fi 

    # Extract pcode name from the directory structure
    pcode_name=$(basename "$(dirname "$(dirname "$csv_file")")")

    # Create the new folder if it doesn't exist
    new_dir="$base_dir/$pcode_name/new_filtered_cleaned"
    mkdir -p "$new_dir"

    # Define the new filename with the simplified name
    new_file="$new_dir/$pcode_name.nodupes.filtered_cleaned.csv.bz2"

    # Copy the file to the new directory and rename it
    cp "$csv_file" "$new_file"

    echo "Copied and renamed: $csv_file -> $new_file"

done

echo "File copying and renaming completed."
