import subprocess

# Print statement signaling the beginning of the script run
# print("start") 

# Run the bash script to push files for each PCODE in series
# subprocess.call('/mnt/code/Benchmarking/filter_clean_booking_summary_stats.sh')


# Print statement signaling the successful completion of the run
# print("end")

file_paths = [
    '/mnt/code/Benchmarking/filter_clean_booking_summary_stats_batch1.sh',
    '/mnt/code/Benchmarking/filter_clean_booking_summary_stats_batch2.sh',
    '/mnt/code/Benchmarking/filter_clean_booking_summary_stats_batch3.sh',
    '/mnt/code/Benchmarking/filter_clean_booking_summary_stats_batch4.sh',
    '/mnt/code/Benchmarking/filter_clean_booking_summary_stats_batch5.sh',
    '/mnt/code/Benchmarking/filter_clean_booking_summary_stats_batch1.sh'
]
for path in file_paths:
    # Print statement signaling the beginning of the script run
    print(f"Start of {path}")
    # Run the bash script to push files for each PCODE in series
    subprocess.call(path)
    # Print statement signaling the successful completion of the run
    print(f"End of {path}")