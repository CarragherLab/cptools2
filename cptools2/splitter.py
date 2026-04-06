import polars as pl
from parserix import parse as _parse


def _well_site_table(img_list):
    """
    parse metadata from image paths and return a polars dataframe
    of image_path and metadata columns

    Parameters:
    -----------
    img_list: list
        list of image paths

    Returns:
    --------
    polars DataFrame of img_paths and Metadata_well, Metadata_site columns
    """
    final_files = [_parse.img_filename(i) for i in img_list]
    df_img = pl.DataFrame({
        "img_paths": img_list,
        "Metadata_well": [_parse.img_well(i) for i in final_files],
        "Metadata_site": [_parse.img_site(i) for i in final_files]
    })
    return df_img


def _group_images(df_img):
    """
    group a single dataframe into a list of lists, with a list
    per well and site

    Parameters:
    -----------
    df_img: polars DataFrame
        dataframe containing image paths with well and site metadata columns

    Returns:
    --------
    a list of lists, grouped by well and site, sorted by channel
    """
    grouped_list = []
    # Sort by well and site for deterministic ordering
    df_sorted = df_img.sort(["Metadata_well", "Metadata_site"])
    # Partition by well and site
    partitions = df_sorted.partition_by(
        ["Metadata_well", "Metadata_site"], maintain_order=True
    )
    for group in partitions:
        paths = group["img_paths"].to_list()
        channel_nums = [_parse.img_channel(i) for i in paths]
        # create tuple (path, channel_number) and sort by channel number
        sort_im = sorted(list(zip(paths, channel_nums)), key=lambda x: x[1])
        # return only the file-paths back from the list of tuples
        grouped_list.append([i[0] for i in sort_im])
    return grouped_list


def chunks(list_like, job_size):
    """
    generator to split list_like into job_size chunks

    Parameters:
    -----------
    list_like: list
    job_size: int
        how many elements in each chunk

    Returns:
    --------
    generator for returning a list of lists, each sub-list containing
    `job_size` elements (apart from the last sub-list which may contain
    fewer elements)
    """
    for i in range(0, len(list_like), job_size):
        yield list_like[i:i+job_size]


def split(img_list, job_size=96):
    """
    split imagelist into an imagelist per job containing job_size images

    Parameters:
    -----------
    img_list: list
        list of image paths
    job_size: int (default = 96)

    Returns:
    --------
    list of lists
    """
    df_img = _well_site_table(img_list)
    grouped_list = _group_images(df_img)
    return [chunk for chunk in chunks(grouped_list, job_size)]


def split_by_plate(plate_store):
    """
    Group all images per plate into single partitions.

    Parameters:
    -----------
    plate_store: dict
        Dictionary mapping plate names to [plate_path, img_list] pairs,
        as stored in Job.plate_store.

    Returns:
    --------
    dict
        Dictionary mapping plate names to a list containing a single
        partition (a flat list of all image paths for that plate).
    """
    result = {}
    for plate_name, (plate_path, img_list) in plate_store.items():
        # Each plate gets a single partition containing all its images
        if isinstance(img_list, list):
            result[plate_name] = [img_list]
        else:
            result[plate_name] = [list(img_list)]
    return result
