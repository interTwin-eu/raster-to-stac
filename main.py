
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

local_data_folders = [
    "./openeo-localprocessing-data/sample_netcdf",
    "./openeo-localprocessing-data/sample_geotiff",
]
local_conn = LocalConnection(local_data_folders)
local_collection = "./openeo-localprocessing-data/sample_netcdf/S2_L2A_sample.nc"
s2_datacube = local_conn.load_collection(local_collection)
exec_results = s2_datacube.execute()

#print(data.t)
RUNTIIME_str = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[:-3] #datetime.datetime.utcnow().strftime('%Y%m%d')

""" TO UNCOMMENT """
#if not os.path.exists(f"results/{RUNTIIME_str}"):
#    os.mkdir(f"results/{RUNTIIME_str}")


assets = []

media_type = pystac.MediaType.COG  # we could also use rio_stac.stac.get_media_type

# additional properties to add in the item
properties = {}

# datetime associated with the item
input_datetime = None

# name of collection the item belongs to
collection = None
collection_url = None

extensions =[
    f"https://stac-extensions.github.io/projection/{PROJECTION_EXT_VERSION}/schema.json", 
    f"https://stac-extensions.github.io/raster/{RASTER_EXT_VERSION}/schema.json",
    f"https://stac-extensions.github.io/eo/{EO_EXT_VERSION}/schema.json",
]

# Get the time dimension values
time_values = exec_results.t.values


for t in time_values:
    # Convert the time value to a datetime object
    timestamp = pd.Timestamp(t)
    
    # Format the timestamp as a string to use in the file name
    time_str = timestamp.strftime('%Y%m%d%H%M%S')


    # Create a unique directory for each time slice
    time_slice_dir = os.path.join(f"results/{RUNTIIME_str}", time_str)

    time_slice_dir = os.path.join(f"results/res", time_str)
    if not os.path.exists(time_slice_dir):
        os.makedirs(time_slice_dir)

    # Get the band name (you may need to adjust this part based on your data)
    bands = exec_results.bands.values

    print(f"\nts: {t}")
    
    # Cycling all bands
    for band in bands:
        print(f"b: {band}")

        # Define the GeoTIFF file path for this time slice and band
        path = os.path.join(time_slice_dir, f"{band}.tif")
        
        # Write the result to the GeoTIFF file
        exec_results.loc[dict(t=t,bands=band)].rio.to_raster(raster_path=path, driver='COG')

        # Create an asset dictionary for this time slice
        assets.append({"name": band, "path": path, "href": path, "role": None})

        bboxes = []

        pystac_assets = []

        img_datetimes = []

        for asset in assets:
            with rasterio.open(asset["path"]) as src_dst:

                # Get BBOX and Footprint
                dataset_geom = get_dataset_geom(src_dst, densify_pts=0, precision=-1)
                bboxes.append(dataset_geom["bbox"])

                """
                if "start_datetime" not in properties and "end_datetime" not in properties:
                    # Try to get datetime from https://gdal.org/user/raster_data_model.html#imagery-domain-remote-sensing
                    dst_date = src_dst.get_tag_item("ACQUISITIONDATETIME", "IMAGERY")
                    dst_datetime = str_to_datetime(dst_date) if dst_date else None
                    if dst_datetime:
                        img_datetimes.append(dst_datetime)
                """

                proj_info = {
                    f"proj:{name}": value
                    for name, value in get_projection_info(src_dst).items()
                }

                raster_info = {
                    "raster:bands": get_raster_info(src_dst, max_size=1024)
                }

                eo_info = {}
                eo_info = {"eo:bands": get_eobands_info(src_dst)}
                cloudcover = src_dst.get_tag_item("CLOUDCOVER", "IMAGERY")
                if cloudcover is not None:
                    properties.update({"eo:cloud_cover": int(cloudcover)})

                pystac_assets.append(
                    (
                        str(asset["name"]), 
                        pystac.Asset(
                            href=asset["href"] or src_dst.name,
                            media_type=media_type,
                            extra_fields={
                                **proj_info,
                                **raster_info, 
                                **eo_info
                            },
                            roles=asset["role"],
                        ),
                    )
                )
        

        if img_datetimes and not input_datetime:
            input_datetime = img_datetimes[0]
            
        input_datetime = input_datetime or datetime.datetime.utcnow()    

        minx, miny, maxx, maxy = zip(*bboxes)
        bbox = [min(minx), min(miny), max(maxx), max(maxy)]
                    
        # item
        item = pystac.Item(
            id=time_str,
            geometry=bbox_to_geom(bbox),
            bbox=bbox,
            collection=collection,
            stac_extensions=extensions,
            datetime=str_to_datetime(str(t)), #.astype(datetime.datetime),#input_datetime,
            properties=properties,
        )

        print(item.stac_extensions)

        exit(1)

        # if we add a collection we MUST add a link
        if collection:
            item.add_link(
                pystac.Link(
                    pystac.RelType.COLLECTION,
                    collection_url or collection,
                    media_type=pystac.MediaType.JSON,
                )
            )

        for key, asset in pystac_assets:
            item.add_asset(key=key, asset=asset)
            
        item.validate()


        json_str = (json.dumps(item.to_dict(), indent=4))

        #printing metadata.json test output file
        with open(f"{time_slice_dir}/metadata.json", "w+") as metadata:
            metadata.write(json_str) 
        #data.rio.to_raster()
    