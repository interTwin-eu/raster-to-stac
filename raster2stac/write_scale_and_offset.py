"""
Writes Scale factor and Offset values to raster data (Tiff or NetCDF data formats)
and save them to the file metadata profile
Created on: 06-05-2024
Last update:
@author: Rufai Omowunmi Balogun
"""

import sys
import logging
import argparse

import rioxarray
import xarray as xr


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def add_scale_and_offset(
    data_path: str, scale_factor: float = 1.0, offset: float = 0.0, tiff: bool = True
):
    """write and save scale and offset values to datasets

    Args:
        data_path (str): path to the dataset
        scale_factor (float, optional): integer value of the scale_factor. Defaults to 1.0.
        offset (float, optional): integer value of the offset. Defaults to 0.0.
        tiff (bool, optional): Boolean if the dataset is in a TIFF format or not. Defaults to True.
    """
    if tiff:
        rio_ds = rioxarray.open_rasterio(data_path)
        rio_ds.attrs["scale_factor"] = scale_factor
        rio_ds.attrs["offset"] = offset

        # save the scale and offset to data
        rio_ds.rio.to_raster(data_path)
    else:
        xds = xr.open_dataset(data_path)
        xds.attrs["scale_factor"] = scale_factor
        xds.attrs["offset"] = offset
        # save scale and offset to data
        xds.to_netcdf(data_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Write Scale factor \
                                     and Offset values directly to raster \
                                     datasets (TIFF file formats and NetCDFs) "
    )

    parser.add_argument(
        "data_path", type=str, help="path to the tiff or netcdf dataset"
    )
    parser.add_argument(
        "scale_factor",
        type=float,
        help="integer value of the scale_factor. \
                         Defaults to 1.0.",
    )
    parser.add_argument(
        "offset",
        type=float,
        help="integer value of the offset.\
                         Defaults to 0.0.",
    )
    parser.add_argument(
        "tiff",
        type=int,
        help="Boolean if the dataset is in \
                        a TIFF format or not. Defaults to True",
    )

    args = parser.parse_args()
    add_scale_and_offset(args.data_path, args.scale_factor, args.offset, args.tiff)
