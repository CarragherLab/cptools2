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
import shutil
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


def create_batch_scripts(batches, commands_location, logfile_location, config):
    """
    Create individual batch scripts and a master submission script to run them sequentially.
    Each batch is a full sequence: Stage -> Analyze -> Destage -> Join -> Transfer.
    """
    from cptools2.generate_scripts import SafePathScript, load_module_text, make_command_paths, make_join_files_script, make_datastore_transfer_script
    from scissorhands import script_generator
    
    time_now = datetime.now().replace(microsecond=0).strftime("%Y-%m-%d_%H-%M-%S")
    cmd_path = make_command_paths(commands_location)
    
    all_scripts_to_submit = []
    last_job_in_chain_name = None

    for batch in batches:
        batch_id = batch['batch_id']
        plates_in_batch = batch['plates']
        job_hex = script_generator.generate_random_hex()  # Unique hex per batch for unique job names
        
        print(f"Creating scripts for Batch {batch_id}: {', '.join(plates_in_batch)} ({batch['total_size_gb']:.2f} GB)")
        
        # --- Staging ---
        staging_job_name = f"staging_batch_{batch_id}_{job_hex}"
        staging_commands = get_commands_for_plates(cmd_path["staging"], plates_in_batch)
        staging_script = SafePathScript(name=staging_job_name, memory="1G", output=os.path.join(logfile_location, "staging"), tasks=len(staging_commands))
        staging_script += "#$ -q staging\n#$ -p -500\n#$ -tc 20\n"
        if last_job_in_chain_name:
            staging_script += f"#$ -hold_jid {last_job_in_chain_name}\n"
        batch_staging_cmd_file = os.path.join(commands_location, f"staging_batch_{batch_id}.txt")
        with open(batch_staging_cmd_file, 'w') as f:
            f.write('\n'.join(staging_commands))
        staging_script.simple_array_loop(phase="staging", input_file=batch_staging_cmd_file)
        staging_loc = os.path.join(commands_location, f"{time_now}_staging_b{batch_id}_script.sh")
        staging_script.save(staging_loc)
        all_scripts_to_submit.append(staging_loc)
        
        # --- Analysis ---
        analysis_job_name = f"analysis_batch_{batch_id}_{job_hex}"
        analysis_commands = get_commands_for_plates(cmd_path["cp_commands"], plates_in_batch)
        analysis_script = script_generator.AnalysisScript(name=analysis_job_name, tasks=len(analysis_commands), hold_jid_ad=staging_job_name, pe="sharedmem 1", memory="24G", output=os.path.join(logfile_location, "analysis"))
        batch_analysis_cmd_file = os.path.join(commands_location, f"cp_commands_batch_{batch_id}.txt")
        with open(batch_analysis_cmd_file, 'w') as f:
            f.write('\n'.join(analysis_commands))
        analysis_script += load_module_text(is_cellprofiler=True)
        analysis_script.loop_through_file(batch_analysis_cmd_file)
        analysis_loc = os.path.join(commands_location, f"{time_now}_analysis_b{batch_id}_script.sh")
        analysis_script.save(analysis_loc)
        all_scripts_to_submit.append(analysis_loc)

        # --- Destaging ---
        destaging_job_name = f"destaging_batch_{batch_id}_{job_hex}"
        destaging_commands = get_commands_for_plates(cmd_path["destaging"], plates_in_batch)
        destaging_script = SafePathScript(name=destaging_job_name, memory="1G", hold_jid_ad=analysis_job_name, tasks=len(destaging_commands), output=os.path.join(logfile_location, "destaging"))
        batch_destaging_cmd_file = os.path.join(commands_location, f"destaging_batch_{batch_id}.txt")
        with open(batch_destaging_cmd_file, 'w') as f:
            f.write('\n'.join(destaging_commands))
        destaging_script.simple_array_loop(phase="destaging", input_file=batch_destaging_cmd_file)
        destage_loc = os.path.join(commands_location, f"{time_now}_destaging_b{batch_id}_script.sh")
        destaging_script.save(destage_loc)
        all_scripts_to_submit.append(destage_loc)
        last_job_in_chain_name = destaging_job_name

        # --- Join ---
        join_script_loc = make_join_files_script(config=config, commands_location=commands_location, logfile_location=logfile_location, job_hex=f"batch_{batch_id}_{job_hex}", time_now=f"{time_now}_b{batch_id}", dependency_job_name=last_job_in_chain_name, plates_to_join=plates_in_batch)
        if join_script_loc:
            all_scripts_to_submit.append(join_script_loc)
            last_job_in_chain_name = f"join_batch_{batch_id}_{job_hex}"

        # --- Transfer ---
        eddie_source_dir = config.create_command_args["location"]
        transfer_script_loc = make_datastore_transfer_script(config=config, commands_location=commands_location, logfile_location=logfile_location, job_hex=f"batch_{batch_id}_{job_hex}", time_now=f"{time_now}_b{batch_id}", eddie_source_dir=eddie_source_dir, dependency_job_name=last_job_in_chain_name)
        if transfer_script_loc:
            all_scripts_to_submit.append(transfer_script_loc)
            last_job_in_chain_name = f"transfer_batch_{batch_id}_{job_hex}"

    # --- Master Submission Script ---
    submit_script_content = [
        "#!/bin/sh",
        "# Auto-generated master submission script by cptools2.",
        "# This script submits all jobs for all batches in the correct sequence.",
        ""
    ]
    for script_path in all_scripts_to_submit:
        submit_script_content.append(f"qsub {script_path}")

    submit_script_path = os.path.join(commands_location, f"{time_now}_SUBMIT_RUNTIME_BATCHES.sh")
    with open(submit_script_path, 'w') as f:
        f.write('\n'.join(submit_script_content))
    os.chmod(submit_script_path, 0o755)
    
    return submit_script_path

def get_commands_for_plates(command_file, plate_names):
    """
    Reads a command file (e.g., staging.txt) and returns only the lines
    that are associated with the given plate names.
    
    A command is associated with a plate if the plate name is a substring
    of the command line. This is based on the assumption that file paths
    in commands will contain the plate name.
    """
    commands = []
    try:
        with open(command_file, 'r') as f:
            for line in f:
                line = line.strip()
                if any(plate_name in line for plate_name in plate_names):
                    commands.append(line)
    except IOError as e:
        print(f"Warning: Could not read command file {command_file}: {e}")
    return commands


def main():
    """Main runtime batch planning function."""
    parser = argparse.ArgumentParser(description="Runtime batch planner for cptools2")
    parser.add_argument("--commands-dir", required=True, help="Path to commands directory")
    parser.add_argument("--logfiles-dir", required=True, help="Path to logfiles directory")
    parser.add_argument("--config-file", required=True, help="Path to the original YAML config file")
    parser.add_argument("--sample-size", type=int, default=10, help="Number of files to sample for size estimation")
    parser.add_argument("--submit-jobs", action="store_true", help="Automatically submit the generated jobs to SGE.")

    
    args = parser.parse_args()
    
    print("=== cptools2 Runtime Batch Planner ===")
    print(f"Commands directory: {args.commands_dir}")
    print(f"Logfiles directory: {args.logfiles_dir}")
    print()
    
    try:
        # Step 0: Load config
        from cptools2.parse_yaml import parse_config_file
        config = parse_config_file(args.config_file)

        # Step 1: Extract plate information from filelists
        print("Step 1: Analyzing filelist directory...")
        plates = extract_plates_from_filelists(args.commands_dir)
        
        if not plates:
            print("No plates found in filelist directory!")
            sys.exit(1)
        
        # Step 2: Estimate plate sizes
        print("\nStep 2: Estimating plate sizes (sampling {} files per filelist)...".format(args.sample_size))
        plate_sizes = {}
        for plate_name, filelists in plates.items():
            size_bytes = estimate_plate_size(filelists, args.sample_size)
            plate_sizes[plate_name] = size_bytes
            print(f"  {plate_name}: {size_bytes / (1024**3):.2f} GB")
        
        # Step 3: Get available space
        print("\nStep 3: Checking available scratch space...")
        available_space = get_available_scratch_space()
        
        # Step 4: Create batches
        print("\nStep 4: Creating space-safe batches...")
        batches = create_plate_batches(plate_sizes, available_space)
        
        print("\nBatch Plan Summary:")
        for batch in batches:
            print(f"  Batch {batch['batch_id']}: {len(batch['plates'])} plates, {batch['total_size_gb']:.2f} GB")
            for plate in batch['plates']:
                print(f"    - {plate}")
        
        # Step 5: Create batch scripts
        print("\nStep 5: Creating all batch, join, and transfer scripts...")
        submit_script = create_batch_scripts(batches, args.commands_dir, args.logfiles_dir, config)
        
        print("\n🎉 Runtime batch planning complete!")
        print(f"Master submission script created at: {submit_script}")
        
        if args.submit_jobs:
            print("\n--submit-jobs flag detected. Automatically submitting jobs...")
            try:
                # Execute the master submission script
                import subprocess
                result = subprocess.run(["bash", submit_script], check=True, capture_output=True, text=True)
                print("--- Submission Output ---")
                print(result.stdout)
                print("-------------------------")
                print("All jobs submitted successfully.")
            except subprocess.CalledProcessError as e:
                print("Error during automatic submission:")
                print(e.stderr)
                sys.exit(1)
        else:
            print(f"\nTo start the analysis, run: bash {submit_script}")
        
    except Exception as e:
        print(f"Error in runtime batch planning: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main() 