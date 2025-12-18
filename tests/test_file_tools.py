import os

from cptools2.file_tools import _discover_plates_from_raw_data


def test_discover_plates_preserves_underscores(tmp_path):
    """
    Plate discovery should not truncate plate names that contain underscores.
    Expected raw_data dir format: "<plate_name>_<job_id>/".
    """
    raw_data = tmp_path / "raw_data"
    raw_data.mkdir()

    # Plate name contains underscores; job id is numeric.
    (raw_data / "my_plate_name_with_underscores_0001").mkdir()
    (raw_data / "my_plate_name_with_underscores_0002").mkdir()

    plates = _discover_plates_from_raw_data(str(raw_data))
    assert plates == ["my_plate_name_with_underscores"]


