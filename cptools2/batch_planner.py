#!/usr/bin/env python
"""
Runtime Batch Planner for cptools2

Analyzes filelist files on Eddie's staging node to determine plate sizes
and create space-safe batches for sequential execution.

This runs AFTER script generation, ON Eddie, with actual DataStore access.
"""

import os
import sys
import glob
import argparse
import json
import shutil
import subprocess
from datetime import datetime
from collections import defaultdict


def extract_plates_from_filelists(commands_location):
    """
    Analyze filelist directory to extract plate information.
    
    Parameters:
    -----------
    commands_location: str
        Path to commands directory containing filelist subdirectory
        
    Returns:
    --------
    dict: {plate_name: [filelist_files]}
    """
    filelist_dir = os.path.join(commands_location, "filelist")
    if not os.path.exists(filelist_dir):
        raise FileNotFoundError(f"Filelist directory not found: {filelist_dir}")
    
    plates = defaultdict(list)
    
    # Read all filelist files
    filelist_files = glob.glob(os.path.join(filelist_dir, "*"))
    
    for filelist_file in filelist_files:
        if os.path.isfile(filelist_file):
            # Extract plate name from filename pattern
            # Example: "1056-D-100_0" -> plate "1056-D-100"
            basename = os.path.basename(filelist_file)
            if '_' in basename:
                plate_name = basename.rsplit('_', 1)[0]
                plates[plate_name].append(filelist_file)
            else:
                # If no underscore, treat entire basename as plate name
                plates[basename].append(filelist_file)
    
    print(f"Found {len(plates)} plates in filelist directory:")
    for plate_name, files in plates.items():
        print(f"  {plate_name}: {len(files)} filelist files")
    
    return dict(plates)


def estimate_plate_size(plate_filelists, sample_size=10):
    """
    Estimate plate size by sampling files from filelist files.
    
    Parameters:
    -----------
    plate_filelists: list
        List of filelist file paths for this plate
    sample_size: int
        Number of files to sample per filelist for size estimation
        
    Returns:
    --------
    int: Estimated total size in bytes
    """
    total_estimated_size = 0
    total_files_counted = 0
    
    for filelist_path in plate_filelists:
        try:
            with open(filelist_path, 'r') as f:
                file_paths = [line.strip() for line in f if line.strip()]
            
            if not file_paths:
                continue
            
            # Sample files for size estimation
            sample_paths = file_paths[:sample_size] if len(file_paths) > sample_size else file_paths
            sample_total_size = 0
            sample_count = 0
            
            for file_path in sample_paths:
                try:
                    if os.path.exists(file_path):
                        sample_total_size += os.path.getsize(file_path)
                        sample_count += 1
                except (OSError, IOError):
                    continue
            
            if sample_count > 0:
                # Extrapolate total size for this filelist
                avg_file_size = sample_total_size / sample_count
                estimated_filelist_size = avg_file_size * len(file_paths)
                total_estimated_size += estimated_filelist_size
                total_files_counted += len(file_paths)
                
        except (IOError, OSError) as e:
            print(f"Warning: Could not process filelist {filelist_path}: {e}")
            continue
    
    return total_estimated_size


def get_available_scratch_space():
    """
    Get available scratch space in bytes.
    
    Returns:
    --------
    int: Available space in bytes
    """
    scratch_dir = "/exports/eddie/scratch"
    if os.path.exists(scratch_dir):
        total, used, free = shutil.disk_usage(scratch_dir)
        return free
    else:
        # Fallback if scratch directory structure is different
        return shutil.disk_usage("/tmp")[2]


def create_plate_batches(plate_sizes, available_space_bytes, safety_factor=0.5, overhead_factor=1.5):
    """
    Group plates into space-safe batches.
    
    Parameters:
    -----------
    plate_sizes: dict
        {plate_name: size_in_bytes}
    available_space_bytes: int
        Available scratch space in bytes
    safety_factor: float
        Use only this fraction of available space per batch
    overhead_factor: float
        Multiply estimated size by this factor for processing overhead
        
    Returns:
    --------
    list: List of batch dictionaries
    """
    # Calculate safe batch size limit
    batch_size_limit = available_space_bytes * safety_factor
    
    print(f"Available scratch space: {available_space_bytes / (1024**3):.2f} GB")
    print(f"Safe batch size limit (50%): {batch_size_limit / (1024**3):.2f} GB")
    
    # Sort plates by size (largest first for better packing)
    sorted_plates = sorted(plate_sizes.items(), key=lambda x: x[1], reverse=True)
    
    batches = []
    current_batch = []
    current_batch_size = 0
    batch_id = 1
    
    for plate_name, plate_size in sorted_plates:
        # Apply overhead factor
        effective_size = plate_size * overhead_factor
        
        # Check if this plate fits in current batch
        if current_batch_size + effective_size <= batch_size_limit:
            current_batch.append(plate_name)
            current_batch_size += effective_size
        else:
            # Start new batch if current batch is not empty
            if current_batch:
                batches.append({
                    'batch_id': batch_id,
                    'plates': current_batch.copy(),
                    'total_size_bytes': current_batch_size,
                    'total_size_gb': current_batch_size / (1024**3)
                })
                batch_id += 1
            
            # Start new batch with current plate
            current_batch = [plate_name]
            current_batch_size = effective_size
    
    # Add final batch if not empty
    if current_batch:
        batches.append({
            'batch_id': batch_id,
            'plates': current_batch.copy(),
            'total_size_bytes': current_batch_size,
            'total_size_gb': current_batch_size / (1024**3)
        })
    
    return batches


def create_batch_scripts(batches, commands_location, logfile_location):
    """
    Create individual batch scripts and submission script.
    
    Parameters:
    -----------
    batches: list
        List of batch dictionaries
    commands_location: str
        Path to commands directory
    logfile_location: str
        Path to logfiles directory
        
    Returns:
    --------
    str: Path to master submission script
    """
    from cptools2.generate_scripts import SafePathScript, load_module_text, make_logfile_text, make_command_paths
    from scissorhands import script_generator
    
    # Generate job hex for dependencies
    job_hex = script_generator.generate_random_hex()
    
    # Get timestamp
    time_now = datetime.now().replace(microsecond=0)
    time_now = str(time_now).replace(" ", "-")
    
    # Load command paths
    cmd_path = make_command_paths(commands_location)
    
    batch_script_paths = []
    
    for batch in batches:
        batch_id = batch['batch_id']
        
        print(f"Creating scripts for Batch {batch_id}: {', '.join(batch['plates'])} ({batch['total_size_gb']:.2f} GB)")
        
        # Create batch-specific job names
        staging_job_name = f"staging_batch_{batch_id}_{job_hex}"
        analysis_job_name = f"analysis_batch_{batch_id}_{job_hex}"
        destaging_job_name = f"destaging_batch_{batch_id}_{job_hex}"
        
        # Set dependencies (batch 2+ waits for previous batch destaging)
        if batch_id > 1:
            staging_dependency = f"destaging_batch_{batch_id-1}_{job_hex}"
        else:
            staging_dependency = None
        
        # Create staging script
        staging_script = SafePathScript(
            name=staging_job_name,
            memory="1G",
            output=os.path.join(logfile_location, "staging"),
            tasks=1  # We'll set this properly by counting commands
        )
        staging_script += "#$ -q staging\n"
        staging_script += "#$ -p -500\n"
        staging_script += "#$ -tc 20\n"
        
        if staging_dependency:
            staging_script += f"#$ -hold_jid {staging_dependency}\n"
        
        staging_script.simple_array_loop(
            phase="staging",
            input_file=cmd_path["staging"]
        )
        
        staging_loc = os.path.join(commands_location, f"{time_now}_staging_batch_{batch_id}_script.sh")
        staging_script.save(staging_loc)
        
        # Create analysis script
        analysis_script = script_generator.AnalysisScript(
            name=analysis_job_name,
            tasks=1,  # We'll set this properly by counting commands
            hold_jid_ad=staging_job_name,
            pe="sharedmem 1",
            memory="24G",
            output=os.path.join(logfile_location, "analysis")
        )
        analysis_script += load_module_text(is_cellprofiler=True)
        analysis_script.loop_through_file(cmd_path["cp_commands"])
        analysis_script += make_logfile_text(logfile_location,
                                             job_file=f"batch_{batch_id}_{job_hex}",
                                             n_tasks=1)
        analysis_loc = os.path.join(commands_location, f"{time_now}_analysis_batch_{batch_id}_script.sh")
        analysis_script.save(analysis_loc)
        
        # Create destaging script
        destaging_script = SafePathScript(
            name=destaging_job_name,
            memory="1G",
            hold_jid_ad=analysis_job_name,
            tasks=1,  # We'll set this properly by counting commands
            output=os.path.join(logfile_location, "destaging")
        )
        destaging_script.simple_array_loop(phase="destaging",
                                           input_file=cmd_path["destaging"])
        destage_loc = os.path.join(commands_location, f"{time_now}_destaging_batch_{batch_id}_script.sh")
        destaging_script.save(destage_loc)
        
        batch_script_paths.append({
            'batch_id': batch_id,
            'plates': batch['plates'],
            'staging_script': staging_loc,
            'analysis_script': analysis_loc,
            'destaging_script': destage_loc
        })
    
    # Create master submission script
    submit_script_content = []
    submit_script_content.append("#!/bin/sh")
    submit_script_content.append("# Runtime batch submission script")
    submit_script_content.append(f"# Generated: {time_now}")
    submit_script_content.append("# Batches run sequentially to prevent scratch space conflicts")
    submit_script_content.append("")
    
    for batch_info in batch_script_paths:
        batch_id = batch_info['batch_id']
        plates = ", ".join(batch_info['plates'])
        
        submit_script_content.append(f"# Batch {batch_id}: {plates}")
        submit_script_content.append(f"echo 'Submitting batch {batch_id} ({len(batch_info['plates'])} plates)...'")
        submit_script_content.append(f"qsub {batch_info['staging_script']}")
        submit_script_content.append(f"qsub {batch_info['analysis_script']}")
        submit_script_content.append(f"qsub {batch_info['destaging_script']}")
        submit_script_content.append("")
    
    submit_script_content.append(f"echo 'Submitted {len(batches)} plate batches for sequential execution'")
    
    # Save submission script
    submit_script_path = os.path.join(commands_location, f"{time_now}_SUBMIT_RUNTIME_BATCHES.sh")
    with open(submit_script_path, 'w') as f:
        f.write('\n'.join(submit_script_content))
    
    # Make executable
    os.chmod(submit_script_path, 0o755)
    
    return submit_script_path


def main():
    """Main runtime batch planning function."""
    parser = argparse.ArgumentParser(description="Runtime batch planner for cptools2")
    parser.add_argument("--commands-dir", required=True, help="Path to commands directory")
    parser.add_argument("--logfiles-dir", required=True, help="Path to logfiles directory")
    parser.add_argument("--sample-size", type=int, default=10, help="Number of files to sample for size estimation")
    
    args = parser.parse_args()
    
    print("=== cptools2 Runtime Batch Planner ===")
    print(f"Commands directory: {args.commands_dir}")
    print(f"Logfiles directory: {args.logfiles_dir}")
    print()
    
    try:
        # Step 1: Extract plate information from filelists
        print("Step 1: Analyzing filelist directory...")
        plates = extract_plates_from_filelists(args.commands_dir)
        
        if not plates:
            print("No plates found in filelist directory!")
            sys.exit(1)
        
        # Step 2: Estimate plate sizes
        print(f"\nStep 2: Estimating plate sizes (sampling {args.sample_size} files per filelist)...")
        plate_sizes = {}
        for plate_name, filelists in plates.items():
            size_bytes = estimate_plate_size(filelists, args.sample_size)
            plate_sizes[plate_name] = size_bytes
            print(f"  {plate_name}: {size_bytes / (1024**3):.2f} GB")
        
        # Step 3: Get available space
        print(f"\nStep 3: Checking available scratch space...")
        available_space = get_available_scratch_space()
        
        # Step 4: Create batches
        print(f"\nStep 4: Creating space-safe batches...")
        batches = create_plate_batches(plate_sizes, available_space)
        
        print(f"\nBatch Plan Summary:")
        for batch in batches:
            print(f"  Batch {batch['batch_id']}: {len(batch['plates'])} plates, {batch['total_size_gb']:.2f} GB")
            for plate in batch['plates']:
                print(f"    - {plate}")
        
        # Step 5: Create batch scripts
        print(f"\nStep 5: Creating batch scripts...")
        submit_script = create_batch_scripts(batches, args.commands_dir, args.logfiles_dir)
        
        print(f"\n🎉 Runtime batch planning complete!")
        print(f"Execute: bash {submit_script}")
        
    except Exception as e:
        print(f"Error in runtime batch planning: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 