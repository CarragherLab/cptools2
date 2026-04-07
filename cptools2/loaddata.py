"""
Create dataframes/csv-files for CellProfiler's LoadData module
"""

import textwrap

import polars as pl
from cptools2 import utils
from parserix import parse as _parse


class _CompatDataFrame:
    """Thin wrapper around a polars DataFrame providing pandas-compatible
    methods used by downstream modules (job.py, commands.py) that we
    cannot modify.

    Delegates attribute access to the underlying polars DataFrame so
    callers can also use native polars operations.
    """

    def __init__(self, df: pl.DataFrame):
        self._df = df

    # --- pandas-compatible shims used by commands.py / job.py ----------

    def to_csv(self, path, index=False, **kwargs):
        """Write CSV in pandas style.  ``index`` is accepted but ignored
        (polars DataFrames have no row index)."""
        self._df.write_csv(path)

    # --- passthrough to polars DataFrame --------------------------------

    @property
    def shape(self):
        return self._df.shape

    @property
    def height(self):
        return self._df.height

    @property
    def columns(self):
        return self._df.columns

    def __len__(self):
        return len(self._df)

    def __getattr__(self, name):
        # Delegate everything else to the underlying polars DataFrame
        return getattr(self._df, name)

    def __getitem__(self, key):
        result = self._df[key]
        if isinstance(result, pl.DataFrame):
            return _CompatDataFrame(result)
        return result

    def __setitem__(self, key, value):
        # polars DataFrames are immutable; use with_columns instead
        if isinstance(value, pl.Series):
            self._df = self._df.with_columns(value.alias(key))
        elif isinstance(value, list):
            self._df = self._df.with_columns(pl.Series(key, value))
        elif isinstance(key, str):
            # scalar or series-like broadcast
            self._df = self._df.with_columns(pl.lit(value).alias(key))
        else:
            raise TypeError(f"Unsupported assignment type: {type(value)}")

    def equals(self, other):
        other_df = other._df if isinstance(other, _CompatDataFrame) else other
        return self._df.equals(other_df)

    def __repr__(self):
        return repr(self._df)


def create_loaddata(img_list, is_new_ix=False, illum_dir=None):
    """
    create a dataframe suitable for cellprofilers LoadData module

    Parameters:
    -----------
    img_list: list
    is_new_ix: Boolean (default=False)
    illum_dir: string or None (default=None)
        When provided, append FileName_Illum_W1..W5 and
        PathName_Illum_W1..W5 columns pointing to illumination
        correction files in this directory.

    Returns:
    --------
    polars DataFrame (wrapped for pandas compatibility)
    """
    df_long = create_long_loaddata(img_list, is_new_ix)
    wide = cast_dataframe(df_long)
    if illum_dir is not None:
        wide = _append_illum_columns(wide, illum_dir)
    return wide


def create_long_loaddata(img_list, is_new_ix=False):
    """
    create a dataframe of image paths with metadata columns

    Parameters:
    -----------
    img_list: list
        list of image paths
    is_new_ix: Boolean (default=False)
        whether or not the filepaths are from the new ImageXpress
        which alters how they are parsed.

    Returns:
    --------
    polars DataFrame (wrapped for pandas compatibility)
    """
    old_path = False if is_new_ix else True
    just_filenames = [_parse.img_filename(i) for i in img_list]
    df_img = pl.DataFrame({
        "URL": just_filenames,
        "path": [_parse.path(i) for i in img_list],
        "Metadata_platename": [_parse.plate_name(i, old_path=old_path) for i in img_list],
        "Metadata_well": [_parse.img_well(i) for i in just_filenames],
        "Metadata_site": [_parse.img_site(i) for i in just_filenames],
        "Metadata_channel": [_parse.img_channel(i) for i in just_filenames],
        "Metadata_platenum": [_parse.plate_num(i, old_path=old_path) for i in img_list]
    })
    return _CompatDataFrame(df_img)


def cast_dataframe(dataframe, check_nan=True):
    """
    reshape a create_loaddata dataframe from long to wide format

    Parameters:
    -----------
    dataframe: _CompatDataFrame or polars DataFrame
    check_nan: Boolean (default = True)
        whether to raise a warning if the dataframe contains
        any missing values

    Returns:
    --------
    polars DataFrame (wrapped for pandas compatibility)
    """
    # Unwrap if needed
    df = dataframe._df if isinstance(dataframe, _CompatDataFrame) else dataframe

    n_channels = df["Metadata_channel"].n_unique()

    wide_df = df.pivot(
        on="Metadata_channel",
        index=["Metadata_site", "Metadata_well", "Metadata_platenum",
               "Metadata_platename", "path"],
        values="URL",
        aggregate_function="first",
    )
    # rename FileName columns from 1, 2... to FileName_W1, FileName_W2 ...
    rename_map = {}
    for i in range(1, n_channels + 1):
        col_name = str(i)
        if col_name in wide_df.columns:
            rename_map[col_name] = f"FileName_W{i}"
    wide_df = wide_df.rename(rename_map)

    # duplicate PathName for each channel
    for i in range(1, n_channels + 1):
        wide_df = wide_df.with_columns(
            pl.col("path").alias(f"PathName_W{i}")
        )
    wide_df = wide_df.drop("path")

    if check_nan is True:
        if utils.any_nan_values(_CompatDataFrame(df)):
            raise LoadDataError("dataframe contains missing values")
    return _CompatDataFrame(wide_df)


def _append_illum_columns(dataframe, illum_dir):
    """
    Append FileName_Illum_W1..W5 and PathName_Illum_W1..W5 columns.

    For each channel W1..W5 present in the dataframe, creates:
    - FileName_Illum_W<n>: "illum_W<n>.npy"
    - PathName_Illum_W<n>: the provided illum_dir path

    Parameters:
    -----------
    dataframe: _CompatDataFrame
    illum_dir: string
        Directory path where illumination correction files are stored

    Returns:
    --------
    _CompatDataFrame with additional illumination columns
    """
    df = dataframe._df if isinstance(dataframe, _CompatDataFrame) else dataframe
    for i in range(1, 6):
        col_name = f"FileName_W{i}"
        if col_name in df.columns:
            df = df.with_columns([
                pl.lit(f"illum_W{i}.npy").alias(f"FileName_Illum_W{i}"),
                pl.lit(illum_dir).alias(f"PathName_Illum_W{i}"),
            ])
    return _CompatDataFrame(df)


def check_dataframe_size(dataframe, min_rows=None):
    """
    check that a dataframe contains at least `min_rows` of data, raise
    an error if this is not the case.

    Parameters:
    ------------
    dataframe: _CompatDataFrame or polars DataFrame
        dataframe to check
    min_rows: int (default = None)
        minimum number of rows the dataframe should contain. If None then
        an Error will never be raised.

    Returns:
    --------
    Raises a `LoadDataError` or nothing
    """
    df = dataframe._df if isinstance(dataframe, _CompatDataFrame) else dataframe
    if min_rows is None:
        return
    nrow = df.height
    if nrow < min_rows:
        msg = """Too few rows detected in a LoadData dataframe. Expected at
                 least {} rows, actual: {}""".format(min_rows, nrow)
        raise LoadDataError(textwrap.dedent(msg))


class LoadDataError(Exception):
    pass
