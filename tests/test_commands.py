import os
import shlex
from cptools2 import commands

CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "example_dir")
TEST_PATH_PLATE_1 = os.path.join(TEST_PATH, "test-plate-1")


def test_make_cp_cmnd():
    """cptools2.commands.make_cp_cmnd(name, pipeline, location, output_loc)"""
    name = "test_name"
    pipeline = "test_pipeline.cppipe"
    location = "/path/to/test_location"
    output_loc = "/path/to/output_location"
    cmnd = commands.make_cp_cmnd(name, pipeline, location, output_loc)
    # Build loaddata path identically to commands.py so quoting is consistent
    loaddata = os.path.join(location, "loaddata", name + ".csv")
    correct = "cellprofiler -r -c -p {pipeline} --data-file={loaddata} -o {output}".format(
        pipeline=shlex.quote(pipeline),
        loaddata=shlex.quote(loaddata),
        output=shlex.quote(output_loc),
    )
    assert cmnd == correct


def test_make_cp_cmnd_with_spaces():
    """shlex.quote prevents injection even when paths contain spaces"""
    name = "plate with spaces_0"
    pipeline = "/path/with spaces/pipeline.cppipe"
    location = "/path/to/test location"
    output_loc = "/path/to/output location/plate with spaces_0"
    cmnd = commands.make_cp_cmnd(name, pipeline, location, output_loc)
    loaddata = "/path/to/test location/loaddata/plate with spaces_0.csv"
    correct = "cellprofiler -r -c -p {pipeline} --data-file={loaddata} -o {output}".format(
        pipeline=shlex.quote(pipeline),
        loaddata=shlex.quote(loaddata),
        output=shlex.quote(output_loc),
    )
    assert cmnd.replace('\\', '/') == correct


def test_make_cp_cmnd_injection_attempt():
    """shlex.quote must neutralise shell metacharacters in plate names"""
    name = 'plate"; touch /tmp/pwned; echo "'
    pipeline = "/path/pipeline.cppipe"
    location = "/scratch/output"
    output_loc = "/scratch/output/raw_data/" + name
    cmnd = commands.make_cp_cmnd(name, pipeline, location, output_loc)
    # shlex.quote wraps the dangerous path in a single-quoted token
    quoted_output = shlex.quote(output_loc)
    assert quoted_output in cmnd
    # shlex.quote must have determined quoting was needed (starts with single quote)
    assert quoted_output.startswith("'")


def test_make_rsync_cmnd():
    """cptools2.commands.make_rsync_cmnd uses shlex.quote on all paths"""
    plate_loc = "/plate_location"
    filelist_name = "/path/to/filelist"
    img_location = "/path/to/images"
    cmnd = commands.make_rsync_cmnd(plate_loc, filelist_name, img_location)
    assert shlex.quote(filelist_name) in cmnd
    assert shlex.quote(plate_loc) in cmnd
    assert shlex.quote(img_location) in cmnd
    assert cmnd.startswith("rsync")
