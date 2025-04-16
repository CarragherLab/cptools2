"""
file_tools.py - File handling utilities for cptools2

This module extends cptools2 with capabilities for post-processing analysis 
results generated by CellProfiler pipelines. The main functionality involves
automatically joining CSV output files based on patterns specified in the
configuration YAML file.

Key features:
- Automatically combines output files across chunked jobs for each plate
- Supports multiple file patterns (e.g., "Image.csv", "Cells.csv") 
- Creates organized output in a dedicated "joined_files" directory
- Provides detailed logging of the joining process
- Integrates with the cptools2 workflow configuration

These tools are especially useful in high-throughput image analysis workflows
where each plate's data might be spread across multiple output files due to
the chunking process. Rather than manually combining these files after analysis,
this module automates the process based on the original experiment structure.

Usage in YAML configuration:
```
join_files: ["Image.csv", "Cells.csv"]  # Join multiple file types
```
or
```
join_files: "Image.csv"  # Join a single file type
```

If no joining is required, simply omit the join_files parameter.
"""

import os
import glob
import pandas as pd
from cptools2.colours import pretty_print, yellow, purple

def join_plate_files(plate_store, raw_data_location, patterns=None):
    """
    Join result files for each plate based on specified patterns.
    
    Parameters:
    -----------
    plate_store : dict
        Dictionary of plates from Job.plate_store
    raw_data_location : string
        Path to where raw data is stored
    patterns : list or None
        List of file patterns to join (e.g., ["Image.csv", "Cells.csv"])
        If None, no files will be joined
        
    Returns:
    --------
    Dictionary with joined file information
    """
    if not patterns:
        pretty_print("No file joining patterns specified. Skipping file joining.")
        return None
    
    pretty_print("Joining files with patterns: {}".format(
        ", ".join([yellow(pattern) for pattern in patterns])))
    
    results = {}
    plate_names = sorted(plate_store.keys())
    
    for pattern in patterns:
        pretty_print("Processing pattern: {}".format(yellow(pattern)))
        
        for plate_name in plate_names:
            pretty_print("\tProcessing plate: {}".format(purple(plate_name)))
            
            # Find all files matching the pattern for this plate
            search_pattern = os.path.join(raw_data_location, f"{plate_name}_*", pattern)
            matched_files = glob.glob(search_pattern)
            
            if not matched_files:
                pretty_print(f"\tNo files found for plate {purple(plate_name)} with pattern {yellow(pattern)}")
                continue
                
            try:
                # Combine files
                combined_csv = pd.concat([pd.read_csv(f, low_memory=False) for f in matched_files])
                
                # Save to output location
                output_dir = os.path.join(raw_data_location, "joined_files")
                os.makedirs(output_dir, exist_ok=True)
                output_file = os.path.join(output_dir, f"{plate_name}_{pattern}")
                
                combined_csv.to_csv(output_file, index=False, encoding='utf-8-sig')
                
                # Store output info
                if plate_name not in results:
                    results[plate_name] = {}
                results[plate_name][pattern] = {
                    'output_file': output_file,
                    'rows': len(combined_csv),
                    'files_combined': len(matched_files)
                }
                
                pretty_print(f"\tCreated {yellow(output_file)} with {purple(len(combined_csv))} rows from {purple(len(matched_files))} files")
            except Exception as e:
                pretty_print(f"\tError processing pattern {yellow(pattern)} for plate {purple(plate_name)}: {str(e)}")
    
    return results