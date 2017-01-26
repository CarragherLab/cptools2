import os
from cptools2 import create_filelist
from cptools2 import job_splitter
from cptools2 import pre_stage
from cptools2 import utils

class Job(object):
    """job class docstring"""

    def __init__(self):
        self.exp_dir = None
        self.chunked = False
        self.plate_store = dict()
        self.loaddata_store = dict()
        self.has_loaddata = False


    def add_experiment(self, exp_dir):
        """
        add all plates in an experiment to the platestore
        """
        self.exp_dir = exp_dir
        # get the plate names from the experiment directory
        plate_names = os.listdir(exp_dir)
        plate_paths = create_filelist.paths_to_plates(exp_dir)
        img_files = [create_filelist.files_from_plate(p) for p in plate_paths]
        for idx, plate in enumerate(plate_names):
            self.plate_store[plate] = [plate_paths[idx], img_files[idx]]


    def add_plate(self, plates, exp_dir):
        """
        add plate(s) from an experiment to the plate_store
        plates being a string or list of strings of the plate names
        """
        if isinstance(plates, str):
            full_path = exp_dir + plates
            img_files = create_filelist.files_from_plate(full_path)
            self.plate_store[plates] = [full_path, img_files]
        elif isinstance(plates, list):
            full_path = [exp_dir + i for i in plates]
            for idx, plate in enumerate(plates):
                self.plate_store[plate] = [full_path[idx], img_files[idx]]
        else:
            raise ValueError("plates has to be a string of a list of strings")


    def remove_plate(self, plates):
        """
        remove plate(s) from plate_store
        """
        if isinstance(plates, str):
            # single plate
            self.plate_store.pop(plates)
        elif isinstance(plates, list):
            for plate in plates:
                self.plate_store.pop(plate)
        else:
            raise ValueError("plates has to be a string or a list of strings")


    def chunk(self, job_size=96):
        """
        group image list into separate jobs, individually for each plate
        """
        # for each image_list in the platestore, split into chunks of job_size
        for key in self.plate_store:
            chunks = job_splitter.split(self.plate_store[key][1], job_size)
            self.plate_store[key][1] = chunks
        self.chunked = True


    def create_loaddata(self):
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
                    df_loaddata = pre_stage.create_loaddata(unnested)
                    self.loaddata_store[key].append(df_loaddata)
            elif self.chunked is False:
                # still nested by channels and wells
                # flatten these nested lists
                unnested = list(utils.flatten(img_list))
                # just a single dataframe for the whole imagelist
                df_loaddata = pre_stage.create_loaddata(unnested)
                self.loaddata_store[key] = df_loaddata
        self.has_loaddata = True


    def create_commands(self, pipeline, location, commands_location):
        """bit of a beast, TODO: refactor"""
        if self.has_loaddata is False:
            self.create_loaddata()
        cp_commands = []
        rsync_commands = []
        rm_commands = []
        utils.make_output_directories(location=location)
        for plate in self.plate_store:
            for job_num, dataframe in enumerate(self.loaddata_store[plate]):
                name = "{}_{}".format(plate, str(job_num))
                output_loc = os.path.join(location, "raw_data", name)
                img_list = list(utils.flatten(self.plate_store[plate][1][job_num]))
                filelist_name = os.path.join(location, "filelist", name)
                img_location = os.path.join(location, "img_data", name)
                plate_loc = self.plate_store[plate][0]
                plate_loc = os.path.join("/", *plate_loc.split(os.sep)[:-1])
                # append cp commands
                cp_cmnd = utils.make_cp_cmnd(plate, job_num, pipeline,
                                             location, output_loc)
                cp_commands.append(cp_cmnd)
                # write loaddata csv to disk
                utils.write_loaddata(name, location, dataframe)
                # write filelist to disk
                utils.write_filelist(img_list=img_list,
                                     filelist_name=filelist_name)
                # append rsync commands
                rsync_cmnd = utils.make_rsync_cmnd(plate_loc=plate_loc,
                                                   filelist_name=filelist_name,
                                                   img_location=img_location)
                rsync_commands.append(rsync_cmnd)
                # make and append rm command
                rm_cmd = pre_stage.rm_string(directory=img_location)
                rm_commands.append(rm_cmd)
        utils.write_cp_commands(commands_location, cp_commands)
        utils.write_stage_commands(commands_location, rsync_commands)
        utils.write_destage_commands(commands_location, rm_commands)


