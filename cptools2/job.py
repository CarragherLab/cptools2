"""
Job management module for cptools2.

Handles experiment configuration, command generation, and result processing
for CellProfiler analysis on computing clusters.
"""

import os

from cptools2 import colours, commands, filelist, loaddata, splitter, utils
from cptools2.colours import pretty_print


class Job(object):
    """
    class to generate staging, analysis and
    de-stating commands for an SGE array job.
    """

    def __init__(self, is_new_ix):
        self.exp_dir = None
        self.chunked = False
        self.plate_store = dict()
        self.loaddata_store = dict()
        self.has_loaddata = False
        self.is_new_ix = is_new_ix
        self.plate_space_requirements = dict()  # Space per plate
        self.total_experiment_size = 0          # Total space for all plates
        self.plate_batches = []                 # Batches of plates

    def add_experiment(self, exp_dir):
        """
        add all plates in an experiment to the platestore

        Parameters:
        -----------
        exp_dir : string
            path to imageXpress experiment that contains plate sub-directories
        """
        self.exp_dir = exp_dir
        plate_paths = filelist.paths_to_plates(exp_dir)
        plate_names = [i.split(os.sep)[-1] for i in plate_paths]
        img_files = [filelist.files_from_plate(p, is_new_ix=self.is_new_ix) for p in plate_paths]
        for idx, plate in enumerate(plate_names):
            self.plate_store[plate] = [plate_paths[idx], img_files[idx]]

    def add_plate(self, plates, exp_dir):
        """
        add plate(s) from an experiment to the plate_store
        plates being a string or list of strings of the plate names.

        can only add plates from a single experiment directory

        if adding plates from multiple experiments, then use multiple add_plate
        methods

        Parameters:
        -----------
        plates : string or list of strings
            plate names of plates to be added
        exp_dir : string
            path to experiment directory that contains the plates
        """
        if isinstance(plates, str):
            full_path = os.path.join(exp_dir, plates)
            img_files = filelist.files_from_plate(full_path, is_new_ix=self.is_new_ix)
            self.plate_store[plates] = [full_path, img_files]
        elif isinstance(plates, list):
            full_path = [os.path.join(exp_dir, i) for i in plates]
            img_files = [filelist.files_from_plate(plate, is_new_ix=self.is_new_ix) for plate in full_path]
            for idx, plate in enumerate(plates):
                self.plate_store[plate] = [full_path[idx], img_files[idx]]
        else:
            raise ValueError("plates has to be a string of a list of strings")

    def remove_plate(self, plates):
        """
        remove plate(s) from plate_store

        Parameters:
        -----------
        plates : string or list of strings
            plate names of plates to be removed
        """
        if isinstance(plates, str):
            self.plate_store.pop(plates)
        elif isinstance(plates, list):
            for plate in plates:
                self.plate_store.pop(plate)
        else:
            raise ValueError("plates has to be a string or a list of strings")

    def chunk(self, job_size=96):
        """
        group image list into separate jobs, individually for each plate

        Parameters:
        -----------
        job_size : int (default=96)
            number of imagesets per job
        """
        # for each image_list in the platestore, split into chunks of job_size
        for key in self.plate_store:
            chunks = splitter.split(self.plate_store[key][1], job_size)
            self.plate_store[key][1] = chunks
        self.chunked = True

    def _create_loaddata(self, job_size=None):
        """
        create dictionary store of loaddata modules
        pretty much mirroring self.plate_store but dataframes instead of
        list of lists
        """
        for key in self.plate_store:
            self.loaddata_store[key] = []
            img_list = self.plate_store[key][1]
            if self.chunked is True:
                # create a dataframe for each chunk in the imagelist
                for index, chunk in enumerate(img_list, 1):
                    # unnest channel groupings
                    # only there before chunking to keep images together
                    unnested = list(utils.flatten(chunk))
                    df_loaddata = loaddata.create_loaddata(unnested, is_new_ix=self.is_new_ix)
                    if index < len(img_list):
                        loaddata.check_dataframe_size(df_loaddata, job_size)
                    self.loaddata_store[key].append(df_loaddata)
            elif self.chunked is False:
                # still nested by channels and wells
                # flatten these nested lists
                unnested = list(utils.flatten(img_list))
                # just a single dataframe for the whole imagelist
                df_loaddata = loaddata.create_loaddata(unnested, is_new_ix=self.is_new_ix)
                self.loaddata_store[key] = df_loaddata
        self.has_loaddata = True

    def _process_plate_job(self, plate, job_num, dataframe, pipeline, location, job_size):
        """Processes a single job within a plate, generating commands and files."""
        name = f"{plate}_{job_num}"
        output_loc = os.path.join(location, "raw_data", name)
        # Ensure img_list corresponds to the correct job_num and is flattened
        img_list = list(utils.flatten(self.plate_store[plate][1][job_num]))
        filelist_name = os.path.join(location, "filelist", name)
        img_location = os.path.join(location, "img_data", name)
        plate_loc_orig = self.plate_store[plate][0]
        # make sure filepath has a leading forward-slash and remove
        # the actual plate name or otherwise the rsync commands ends
        # with the plate-name duplicated
        plate_loc = os.path.join("/", *plate_loc_orig.split(os.sep)[:-1])

        # Generate commands
        cp_cmnd = commands.make_cp_cmnd(name=name, pipeline=pipeline,
                                        location=location,
                                        output_loc=output_loc)
        rsync_cmnd = commands.make_rsync_cmnd(plate_loc=plate_loc,
                                                filelist_name=filelist_name,
                                                img_location=img_location)
        rm_cmd = commands.rm_string(directory=img_location)

        # Write auxiliary files
        commands.write_loaddata(name=name, location=location,
                                dataframe=dataframe)
        commands.write_filelist(img_list=img_list,
                                filelist_name=filelist_name)

        return cp_cmnd, rsync_cmnd, rm_cmd

    def _process_plate(self, plate, pipeline, location, job_size):
        """Processes all jobs for a single plate, collecting commands."""
        plate_cp_commands = []
        plate_rsync_commands = []
        plate_rm_commands = []

        print(colours.purple("\t Processing plate:"), colours.yellow(f"{plate}"))
        # Iterate through jobs for the current plate
        for job_num, dataframe in enumerate(self.loaddata_store[plate]):
            # Call helper to process the job and get commands
            cp_cmnd, rsync_cmnd, rm_cmd = self._process_plate_job(
                plate, job_num, dataframe, pipeline, location, job_size
            )
            # Append commands to the plate's command lists
            plate_cp_commands.append(cp_cmnd)
            plate_rsync_commands.append(rsync_cmnd)
            plate_rm_commands.append(rm_cmd)

        return plate_cp_commands, plate_rsync_commands, plate_rm_commands

    def _write_and_check_commands(self, commands_location, rsync_commands, cp_commands, rm_commands):
        """Writes command lists to files and checks their validity."""
        # Print status updates
        pretty_print("creating image filelist")
        # Note: LoadData CSVs created earlier. Now writing command files.
        pretty_print("writing command files...")
        pretty_print("creating staging commands")
        pretty_print("creating Cellprofiler commands")
        pretty_print("creating destaging commands")

        # Write commands to disk
        commands.write_commands(commands_location=commands_location,
                                rsync_commands=rsync_commands,
                                cp_commands=cp_commands,
                                rm_commands=rm_commands)

        # Check commands files are not empty
        names = ["staging", "cp_commands", "destaging"]
        cmnds_files = [os.path.join(commands_location, name + ".txt") for name in names]
        for cmnd_file in cmnds_files:
            commands.check_commands(cmnd_file)

    def _estimate_directory_size(self, start_path, sample_size=10, verbose=False):
        """
        Estimate directory size using statistical sampling of image types.
        Imaging datasets typically contain:
        - Regular images (large files)
        - Thumbnail images (small files with '_thumb' in filename)
        Sample each type separately, calculate statistics, and extrapolate
        using conservative upper bounds (mean + 2*std).
        """
        import os
        import random
        import statistics
        
        thumb_files = []
        regular_files = []
        try:
            # Use a recursive generator function with the faster os.scandir()
            def _fast_scandir(path):
                try:
                    for entry in os.scandir(path):
                        if entry.is_dir(follow_symlinks=False):
                            yield from _fast_scandir(entry.path)
                        elif entry.is_file(follow_symlinks=False):
                            yield entry
                except OSError:
                    # Suppress errors from permission issues on subdirectories
                    pass

            for entry in _fast_scandir(start_path):
                if '_thumb' in entry.name.lower():
                    thumb_files.append(entry.path)
                else:
                    regular_files.append(entry.path)

        except Exception as e:
            if verbose:
                print(f"Warning: Could not analyze directory {start_path}: {e}")
            return 100 * 1024**3  # 100GB fallback

        thumb_stats = self._calculate_file_type_stats(thumb_files, sample_size, "thumbnail")
        regular_stats = self._calculate_file_type_stats(regular_files, sample_size, "regular")
        total_estimated_size = (
            thumb_stats['count'] * thumb_stats['upper_bound'] +
            regular_stats['count'] * regular_stats['upper_bound']
        )
        if verbose:
            print(f"Size estimation for {start_path}:")
            print(f"  Thumbnail files: {thumb_stats['count']} files, "
                  f"{thumb_stats['upper_bound']/1024:.1f}KB upper bound each")
            print(f"  Regular files: {regular_stats['count']} files, "
                  f"{regular_stats['upper_bound']/(1024**2):.1f}MB upper bound each")
            print(f"  Total estimated: {total_estimated_size/(1024**3):.2f}GB")
        return int(total_estimated_size)

    def _calculate_file_type_stats(self, file_list, sample_size, file_type_name):
        """
        Calculate statistics for a specific file type.
        Returns:
        dict : {
            'count': total_file_count,
            'mean': mean_size,
            'std': standard_deviation,
            'upper_bound': mean + 2*std
        }
        """
        import statistics
        import random
        if not file_list:
            return {
                'count': 0,
                'mean': 0,
                'std': 0,
                'upper_bound': 0
            }
        sample_files = random.sample(file_list, min(sample_size, len(file_list)))
        sample_sizes = []
        for filepath in sample_files:
            try:
                size = os.path.getsize(filepath)
                sample_sizes.append(size)
            except (OSError, IOError):
                continue
        if not sample_sizes:
            default_size = 50 * 1024**2 if file_type_name == "regular" else 100 * 1024  # 50MB or 100KB
            return {
                'count': len(file_list),
                'mean': default_size,
                'std': default_size * 0.5,
                'upper_bound': default_size * 1.5
            }
        mean_size = statistics.mean(sample_sizes)
        std_size = statistics.stdev(sample_sizes) if len(sample_sizes) > 1 else mean_size * 0.3
        upper_bound = mean_size + (2 * std_size)
        return {
            'count': len(file_list),
            'mean': mean_size,
            'std': std_size,
            'upper_bound': upper_bound
        }

    def calculate_plate_sizes(self):
        """Calculate space requirements with minimal output (progress handled in main workflow)"""
        total_size = 0
        for plate_name, plate_data in self.plate_store.items():
            plate_path = plate_data[0]
            plate_size = self._estimate_directory_size(plate_path, verbose=False)
            self.plate_space_requirements[plate_name] = plate_size
            total_size += plate_size
            # Print plate name and size in yellow
            pretty_print(f"\t {colours.yellow(plate_name)}: {colours.yellow(f'{plate_size/(1024**3):.2f}GB')}")
        self.total_experiment_size = total_size
        return self.plate_space_requirements

    def create_plate_batches(self, available_scratch_space):
        """
        Group plates into batches based on space constraints
        Each batch will fit within 50% of available scratch space
        """
        # Use 50% of available space per batch
        max_batch_size = available_scratch_space * 0.5
        
        # Account for processing overhead
        overhead_factor = 1.5  # 50% overhead for temp files
        
        # Sort plates by size (largest first for better packing)
        sorted_plates = sorted(
            self.plate_space_requirements.items(), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        current_batch = []
        current_batch_size = 0
        batch_number = 1
        
        for plate_name, plate_size in sorted_plates:
            effective_plate_size = plate_size * overhead_factor
            
            # Check if this plate fits in current batch
            if current_batch_size + effective_plate_size <= max_batch_size:
                current_batch.append(plate_name)
                current_batch_size += effective_plate_size
            else:
                # Start new batch if current batch has plates
                if current_batch:
                    self.plate_batches.append({
                        'batch_id': batch_number,
                        'plates': current_batch.copy(),
                        'total_size_gb': current_batch_size / (1024**3),
                        'plate_count': len(current_batch)
                    })
                    batch_number += 1
                
                # Start new batch with current plate
                current_batch = [plate_name]
                current_batch_size = effective_plate_size
        
        # Add final batch if it has plates
        if current_batch:
            self.plate_batches.append({
                'batch_id': batch_number,
                'plates': current_batch.copy(),
                'total_size_gb': current_batch_size / (1024**3),
                'plate_count': len(current_batch)
            })
        
        return self.plate_batches

    def get_plates_for_batch(self, batch_id):
        """Returns the list of plates for a given batch ID."""
        if not self.plate_batches:
            raise ValueError("Plate batches have not been created yet. Call create_plate_batches() first.")
        if not 0 <= batch_id < len(self.plate_batches):
            raise ValueError(f"Invalid batch_id: {batch_id}. Must be between 0 and {len(self.plate_batches) - 1}.")
        return self.plate_batches[batch_id]

    def create_commands(self, pipeline, location, commands_location, job_size, enable_batching=False, available_scratch_space=None):
        """
        Enhanced to support batch-aware command generation.
        """
        if not os.path.isfile(pipeline):
            raise FileNotFoundError(f"Pipeline file not found: {pipeline}")
        if enable_batching and available_scratch_space:
            self.create_plate_batches(available_scratch_space)
            self._create_batch_command_files(pipeline, location, commands_location, job_size)
        else:
            # Original single-batch workflow
            pretty_print("creating image list")
            if self.has_loaddata is False:
                self._create_loaddata(job_size)
            cp_commands, rsync_commands, rm_commands = [], [], []
            pretty_print("creating output directories at {}".format(colours.yellow(location)))
            commands.make_output_directories(location=location)
            platenames = sorted(self.plate_store.keys())
            pretty_print("detected {} {}".format(colours.yellow(len(platenames)), colours.purple("plates")))
            for plate in platenames:
                p_cp, p_rsync, p_rm = self._process_plate(plate, pipeline, location, job_size)
                cp_commands.extend(p_cp)
                rsync_commands.extend(p_rsync)
                rm_commands.extend(p_rm)
            self._write_and_check_commands(
                commands_location=commands_location,
                rsync_commands=rsync_commands,
                cp_commands=cp_commands,
                rm_commands=rm_commands,
            )

    def _create_batch_command_files(self, pipeline, location, commands_location, job_size):
        """
        Generate separate command files for each batch.
        Creates: staging_batch_1.txt, cp_commands_batch_1.txt, etc.
        """
        for batch in self.plate_batches:
            batch_id = batch['batch_id']
            plates_in_batch = batch['plates']
            batch_plate_store = {k: v for k, v in self.plate_store.items() if k in plates_in_batch}
            # Temporarily replace plate_store for batch processing
            original_plate_store = self.plate_store
            self.plate_store = batch_plate_store
            # Always rebuild loaddata_store for the current batch
            self.loaddata_store = {}
            self.has_loaddata = False
            self._create_loaddata(job_size)
            cp_commands, rsync_commands, rm_commands = [], [], []
            commands.make_output_directories(location=location)
            platenames = sorted(self.plate_store.keys())
            for plate in platenames:
                p_cp, p_rsync, p_rm = self._process_plate(plate, pipeline, location, job_size)
                cp_commands.extend(p_cp)
                rsync_commands.extend(p_rsync)
                rm_commands.extend(p_rm)
            # Write batch-specific command files
            def batch_file(name):
                return os.path.join(commands_location, f"{name}_batch_{batch_id}.txt")
            commands.write_commands(
                commands_location=commands_location,
                rsync_commands=rsync_commands,
                cp_commands=cp_commands,
                rm_commands=rm_commands,
                staging_file=batch_file("staging"),
                cp_commands_file=batch_file("cp_commands"),
                destaging_file=batch_file("destaging")
            )
            # Restore original plate_store
            self.plate_store = original_plate_store

    def join_results(self, location, patterns=None):
        """
        Join result files for each plate based on specified patterns.
        
        Parameters:
        -----------
        location : string
            Path to where the results are stored
        patterns : list or None
            List of file patterns to join (e.g., ["Image.csv", "Cells.csv"])
            If None, no files will be joined
            
        Returns:
        --------
        Dictionary with joined file information
        """
        from cptools2.file_tools import join_plate_files
        
        raw_data_location = os.path.join(location, "raw_data")
        return join_plate_files(self.plate_store, raw_data_location, patterns)