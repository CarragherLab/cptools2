import os

from cptools2 import job


def test_job_add_plate_autodetect_layout_old():
    """Job.add_plate should auto-detect old IX layout regardless of Job.is_new_ix"""
    current_path = os.path.dirname(__file__)
    exp_dir = os.path.join(current_path, "example_dir")
    plate_name = "test-plate-1"

    # Intentionally set to True to ensure per-plate detection wins.
    j = job.Job(is_new_ix=True)
    j.add_plate(plates=[plate_name], exp_dir=exp_dir)

    assert plate_name in j.plate_store
    assert j.plate_is_new_ix[plate_name] is False
    assert len(j.plate_store[plate_name][1]) > 0


