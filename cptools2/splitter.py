from parserix import parse as _parse
import pandas as _pd


def _well_site_table(img_list):
    """return pandas dataframe with metadata columns"""
    final_files = [_parse.img_filename(i) for i in img_list]
    df_img = _pd.DataFrame({
        "img_paths"     : img_list,
        "Metadata_well" : [_parse.img_well(i) for i in final_files],
        "Metadata_site" : [_parse.img_site(i) for i in final_files]
        })
    return df_img


def _group_images(df_img):
    """group images by well and site"""
    grouped_list = []
    for _, group in  df_img.groupby(["Metadata_well", "Metadata_site"]):
        grouped = list(group["img_paths"])
        channel_nums = [_parse.img_channel(i) for i in grouped]
        # create tuple (path, channel_number) and sort by channel number
        sort_im = sorted(list(zip(grouped, channel_nums)), key=lambda x: x[1])
        # return on the file-paths back from the list of tuples
        grouped_list.append([i[0] for i in sort_im])
    return grouped_list


def chunks(list_like, job_size):
    """generator to split list_like into job_size chunks"""
    for i in range(0, len(list_like), job_size):
        yield list_like[i:i+job_size]


def split(img_list, job_size=96):
    """split imagelist into an imagelist per job containing job_size images"""
    df_img = _well_site_table(img_list)
    grouped_list = _group_images(df_img)
    return [chunk for chunk in chunks(grouped_list, job_size)]
