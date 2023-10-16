
from openeo.local import LocalConnection
import rioxarray as rio
import datetime 
import os 
import datetime

import pystac
from pystac.utils import str_to_datetime

import rasterio

# Import extension version
from rio_stac.stac import PROJECTION_EXT_VERSION, RASTER_EXT_VERSION, EO_EXT_VERSION

# Import rio_stac methods
from rio_stac.stac import (
    get_dataset_geom,
    get_projection_info,
    get_raster_info,
    get_eobands_info,
    bbox_to_geom,
)
import pandas as pd
import json
import sys
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from raster2stac import raster2stac as r2slib


local_data_folders = [
    "/home/lmercurio/dev/test-rio-stac/openeo-localprocessing-data/sample_netcdf",
    "/home/lmercurio/dev/test-rio-stac/openeo-localprocessing-data/sample_geotiff",
]
local_conn = LocalConnection(local_data_folders)
local_collection = "/home/lmercurio/dev/test-rio-stac/openeo-localprocessing-data/sample_netcdf/S2_L2A_sample.nc"
s2_datacube = local_conn.load_collection(local_collection)
exec_results = s2_datacube.execute()


r2s = r2slib.Raster2STAC(exec_results, output_folder='./results/', collection_id="test-id",
                                description="Description", output_file='test_collection.json',
                                stac_version="1.0.0", verbose=True)

r2s.generate_stac()
