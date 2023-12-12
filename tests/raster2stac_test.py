
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
    "../data/test_basil",
]
local_conn = LocalConnection(local_data_folders)
#local_collection = "../data/test_basil/Bosco_2021_2023.nc"
local_collection = "../data/test_michele/S2_L2A_sample.nc"

s2_datacube = local_conn.load_collection(local_collection)
exec_results = s2_datacube.execute()


providers = [{
    "url":"http://www.eurac.edu",
    "name":"Eurac EO WCS",
    "roles":["host"]
    }]


r2s = r2slib.Raster2STAC(exec_results, output_folder='./results/', collection_id="test-collection-1", description="Description",
                                output_file='test_collection.json', stac_version="1.0.0", verbose=True, s3_upload=False, version="1.0",
                                providers=providers, output_format="csv", title="This is a test collection",
                                collection_url="https://url-to-coll.col/collection", license="test-license", write_json_items = True,
                                keywords=['key1', 'key2', 'key3', 'key4'], sci_citation='N/A',
                                #url_collection=''
                                )

r2s.generate_stac()
