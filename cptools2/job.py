import os
from itertools import chain
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
                    unnested = list(chain.from_iterable(chunk))
                    df_loaddata = pre_stage.create_loaddata(unnested)
                    self.loaddata_store[key].append(df_loaddata)
            elif self.chunked is False:
                # still nested by channels and wells
                # flatten these nested lists
                unnested = list(chain.from_iterable(img_list))
                # just a single dataframe for the whole imagelist
                df_loaddata = pre_stage.create_loaddata(unnested)
                self.loaddata_store[key] = df_loaddata


    def create_commands(self, pipeline, location, commands_location):
        """bit of a beast, TODO: refactor"""
        cp_commands = []
        rsync_commands = []
        rm_commands = []
        utils.make_dir(os.path.join(location, "raw_data"))
        for plate in self.plate_store:
            for job_num, dataframe in enumerate(self.loaddata_store[plate]):
                name = "{}_{}".format(plate, str(job_num))
                # write loaddata dataframe to disk
                utils.make_dir(os.path.join(location, "loaddata"))
                loaddata_name = os.path.join(location, "loaddata", name)
                dataframe.to_csv(loaddata_name, index=False)
                # create and append cp command
                output_loc = os.path.join(location, "raw_data", name)
                cp_cmnd = pre_stage.cp_command(pipeline=pipeline,
                                               load_data=loaddata_name+".csv",
                                               output_location=output_loc)
                cp_commands.append(cp_cmnd)
                # create filelist
                utils.make_dir(os.path.join(location, "filelist"))
                img_list = list(chain(*self.plate_store[plate][1][job_num]))
                filelist_name = os.path.join(location, "filelist", name)
                with open(filelist_name, "w") as f:
                    for line in img_list:
                        f.write(line + "\n")
                # create and append rsync commands
                # source will be the plate location
                utils.make_dir(os.path.join(location, "img_data"))
                img_location = os.path.join(location, "img_data", name)
                plate_loc = self.plate_store[plate][0]
                # trim plate name from plate_loc
                plate_loc = os.path.join("/", *plate_loc.split(os.sep)[:-1])
                rsync_cmd = pre_stage.rsync_string(filelist=filelist_name,
                                                   source=plate_loc,
                                                   destination=img_location)
                rsync_commands.append(rsync_cmd)
                # create and append rm commands
                # need to rm the img_location
                rm_cmd = pre_stage.rm_string(directory=img_location)
                rm_commands.append(rm_cmd)
        # write the commands to disk
        # cellprofiler commands
        cp_cmnd_loc = os.path.join(commands_location, "cp_commands.txt")
        with open(cp_cmnd_loc, "w") as cp:
            for line in cp_commands:
                cp.write(line + "\n")
        # staging commands
        rsync_cmnd_loc = os.path.join(commands_location, "staging.txt")
        with open(rsync_cmnd_loc, "w") as r:
            for line in rsync_commands:
                r.write(line + "\n")
        # destaging commands
        rm_cmnd_loc = os.path.join(commands_location, "destaging.txt")
        with open(rm_cmnd_loc, "w") as d:
            for line in rm_commands:
                d.write(line + "\n")

