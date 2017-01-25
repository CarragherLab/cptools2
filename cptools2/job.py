import os
import itertools
from cptools2 import create_filelist
from cptools2 import job_splitter
from cptools2 import pre_stage

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
        self.chunked=True


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
                    unnested = list(itertools.chain.from_iterable(chunk))
                    df_loaddata = pre_stage.create_loaddata(unnested)
                    self.loaddata_store[key].append(df_loaddata)
            elif self.chunked is False:
                # still nested by channels and wells
                unnested = list(itertools.chain.from_iterable(img_list))
                # just a single dataframe for the whole imagelist
                df_loaddata = pre_stage.create_loaddata(unnested)
                self.loaddata_store[key] = df_loaddata









