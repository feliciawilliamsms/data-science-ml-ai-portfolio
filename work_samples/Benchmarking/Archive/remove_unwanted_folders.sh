#!/bin/bash

# Base directory
base_dir="/mnt/data/Fee_Recovery_2024"

# List folders to remove
# folders_to_remove=("booking_curve_2" "summary_stats_2")
folder_to_remove="filtered_cleaned_2"

# Find and remove folders
# for folder in "${folders_to_remove[@]}"; do 
#     find "$base_dir" -type d -name "$folder" -exec rm -rf {} + -print
# done

# Find and remove only the filtered_cleaned_3 folders
find "$base_dir"/*/ -type d -name "$folder_to_remove" -exec rm -rf {} + -print

echo "Remove all $folder_to_remove folders."


# # Loop through each PCODE directory
# for pcode_dir in "$base_dir"/*/; do
#     # Loop through each folder to remove 
#     for folder in "${folders_to_remove[@]}"; do
#         # Construc the full path to the folder
#         folder_path="${pcode_dir}${folder}"

#         # Check if the folder exists
#         if  [ -d "folder_path" ]; then
#             # Remove the folder and its contents
#             rm -rf "$folder_path"
#             echo "Removed $folder_path"
#         else
#             echo "$folder_path does not exist." 
#         fi 
#     done 
# done