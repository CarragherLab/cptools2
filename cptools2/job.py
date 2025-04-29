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

    def create_commands(self, pipeline, location, commands_location, job_size):
        """
        Orchestrates the creation of staging, analysis, and destaging commands.

        Ensures LoadData is generated, creates output directories,
        processes each plate and job via helper methods, and writes
        the final command files.

        Parameters:
        ------------
        pipeline : string
            Path to the CellProfiler pipeline file.
        location : string
            Base directory for storing LoadData CSVs, intermediate image
            data, and final results.
        commands_location: string
            Directory where the staging, cp_commands, and destaging
            command files will be written.
        job_size : int
            Used for LoadData generation and validation if chunking occurred.
        """
        # --- Input Validation ---
        if not os.path.isfile(pipeline):
            raise FileNotFoundError(f"Pipeline file not found: {pipeline}")
        # Potentially add more checks for location/commands_location if needed

        # --- Processing Start ---
        pretty_print("creating image list")
        if self.has_loaddata is False:
            self._create_loaddata(job_size)
        cp_commands, rsync_commands, rm_commands = [], [], []
        pretty_print("creating output directories at {}".format(colours.yellow(location)))
        commands.make_output_directories(location=location)
        # for each job per plate, create loaddata and commands
        platenames = sorted(self.plate_store.keys())
        pretty_print("detected {} {}".format(
            colours.yellow(len(platenames)),
            colours.purple("plates"))
        )
        # Process each plate using the helper method
        for plate in platenames:
            # Call helper to process the plate and get commands
            plate_cp_commands, plate_rsync_commands, plate_rm_commands = self._process_plate(
                plate, pipeline, location, job_size
            )
            # Extend the main command lists with commands from this plate
            cp_commands.extend(plate_cp_commands)
            rsync_commands.extend(plate_rsync_commands)
            rm_commands.extend(plate_rm_commands)

        # Write commands to disk and check files
        self._write_and_check_commands(commands_location, rsync_commands, cp_commands, rm_commands)

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