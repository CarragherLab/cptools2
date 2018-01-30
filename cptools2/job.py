import os
from cptools2 import filelist
from cptools2 import splitter
from cptools2 import loaddata
from cptools2 import commands
from cptools2 import utils

class Job(object):
    """
    class to generate staging, analysis and
    de-stating commands for an SGE array job.
    """

    def __init__(self):
        self.exp_dir = None
        self.chunked = False
        self.plate_store = dict()
        self.loaddata_store = dict()
        self.has_loaddata = False


    def add_experiment(self, exp_dir):
        """
        add all plates in an experiment to the platestore

        Parameters:
        -----------
        exp_dir : string
            path to imageXpress experiment that contains plate sub-directories
        """
        self.exp_dir = exp_dir
        plate_names = os.listdir(exp_dir)
        plate_paths = filelist.paths_to_plates(exp_dir)
        img_files = [filelist.files_from_plate(p) for p in plate_paths]
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
            img_files = filelist.files_from_plate(full_path)
            self.plate_store[plates] = [full_path, img_files]
        elif isinstance(plates, list):
            full_path = [os.path.join(exp_dir, i) for i in plates]
            img_files = [filelist.files_from_plate(plate) for plate in full_path]
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
                for chunk in img_list:
                    # unnest channel groupings
                    # only there before chunking to keep images together
                    unnested = list(utils.flatten(chunk))
                    df_loaddata = loaddata.create_loaddata(unnested)
                    loaddata.check_dataframe_size(df_loaddata, job_size)
                    self.loaddata_store[key].append(df_loaddata)
            elif self.chunked is False:
                # still nested by channels and wells
                # flatten these nested lists
                unnested = list(utils.flatten(img_list))
                # just a single dataframe for the whole imagelist
                df_loaddata = loaddata.create_loaddata(unnested)
                loaddata.check_dataframe_size(loaddata, job_size)
                self.loaddata_store[key] = df_loaddata
        self.has_loaddata = True


    def create_commands(self, pipeline, location, commands_location, job_size):
        """
        bit of a beast, TODO: refactor

        create stage, analysis and destage commands and write to disk

        Parameters:
        ------------
        pipeline : string
            path to cellprofiler pipeline
        location : string
            file path to location in which the loaddata, images and results
            will be stored
        commands_location: string
            file path to location in which to store the stage, analysis and
            destage commands.
        """
        if self.has_loaddata is False:
            self._create_loaddata(job_size)
        cp_commands, rsync_commands, rm_commands = [], [], []
        print("** creating output directories at '{}'".format(location))
        commands.make_output_directories(location=location)
        # for each job per plate, create loaddata and commands
        platenames = sorted(self.plate_store.keys())
        print("** detected {} plates in experiment".format(len(platenames)))
        for i, plate in enumerate(platenames, 1):
            print("\t {}. {}".format(i, plate))
            for job_num, dataframe in enumerate(self.loaddata_store[plate]):
                name = "{}_{}".format(plate, str(job_num))
                output_loc = os.path.join(location, "raw_data", name)
                img_list = list(utils.flatten(self.plate_store[plate][1][job_num]))
                filelist_name = os.path.join(location, "filelist", name)
                img_location = os.path.join(location, "img_data", name)
                plate_loc = self.plate_store[plate][0]
                # make sure filepath has a leading forward-slash and remove
                # the actual plate name or otherwise the rsync commands ends
                # with the plate-name duplicated
                plate_loc = os.path.join("/", *plate_loc.split(os.sep)[:-1])
                # append cp commands
                cp_cmnd = commands.make_cp_cmnd(name=name, pipeline=pipeline,
                                                location=location,
                                                output_loc=output_loc)
                cp_commands.append(cp_cmnd)
                # write loaddata csv to disk
                commands.write_loaddata(name=name, location=location,
                                        dataframe=dataframe)
                # write filelist to disk
                commands.write_filelist(img_list=img_list,
                                        filelist_name=filelist_name)
                # append rsync commands
                rsync_cmnd = commands.make_rsync_cmnd(plate_loc=plate_loc,
                                                      filelist_name=filelist_name,
                                                      img_location=img_location)
                rsync_commands.append(rsync_cmnd)
                # make and append rm command
                rm_cmd = commands.rm_string(directory=img_location)
                rm_commands.append(rm_cmd)
        # write commands to disk as a txt file
        print("** creating image filelist")
        print("** creating csv files for LoadData")
        print("** creating staging commands")
        print("** creating Cellprofiler commands")
        print("** creating destaging commands")
        commands.write_commands(commands_location=commands_location,
                                rsync_commands=rsync_commands,
                                cp_commands=cp_commands,
                                rm_commands=rm_commands)
        # check commands files are not empty, raise an error if they are
        names = ["staging", "cp_commands", "destaging"]
        cmnds_files = [os.path.join(commands_location, name + ".txt") for name in names]
        for cmnd_file in cmnds_files:
            commands.check_commands(cmnd_file)

