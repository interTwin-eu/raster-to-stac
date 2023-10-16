import datetime 
import os 
import pystac
from pystac.utils import str_to_datetime
import rasterio
from pathlib import Path
import copy

# Import extension version
from rio_stac.stac import (
    PROJECTION_EXT_VERSION,
    RASTER_EXT_VERSION,
    EO_EXT_VERSION
)

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
import xarray as xr
from typing import Callable, Optional, Union
import logging

import boto3
import botocore

_log = logging.getLogger(__name__)
#_log.setLevel(logging.INFO)

conf_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'conf.json')

# loading confs from conf.json
with open(conf_path, 'r') as f:
    conf_data = json.load(f)

# loadng AWS credentials/confs
aws_access_key = conf_data["s3"]["aws_access_key"]
aws_secret_key = conf_data["s3"]["aws_secret_key"]
#aws_region    = conf_data["s3"]["aws_region"]'

class Raster2STAC():
    
    def __init__(self,data: xr.DataArray,
                 t_dim: Optional[str] = "t",
                 b_dim: Optional[str] = "bands",
                 collection_id: Optional[str] = None,         #collection id as string (same of collection and items)
                 collection_url: Optional[str] = None,
                 output_folder: Optional[str] = None,
                 output_file: Optional[str] = None,
                 description: Optional[str] = "",
                 stac_version="1.0.0",
                 verbose=False,
                 bucket_name = conf_data["s3"]["bucket_name"],
                 bucket_file_prefix = conf_data["s3"]["bucket_file_prefix"]
                ):
                
        self.data = data
        
        self.t_dim = t_dim
        self.b_dim = b_dim
        
        self.pystac_assets = []

        self.media_type = None

        # additional properties to add in the item
        self.properties = {}

        # datetime associated with the item
        self.input_datetime = None

        # name of collection the item belongs to
        self.collection_id = collection_id
        self.collection_url = collection_url
        self.description = description

        self.stac_version = stac_version


        self.extensions = [
            f"https://stac-extensions.github.io/projection/{PROJECTION_EXT_VERSION}/schema.json", 
            f"https://stac-extensions.github.io/raster/{RASTER_EXT_VERSION}/schema.json",
            f"https://stac-extensions.github.io/eo/{EO_EXT_VERSION}/schema.json",
        ]
        
        self.set_media_type(pystac.MediaType.COG)  # we could also use rio_stac.stac.get_media_type)
        
        if output_folder is not None:
            self.output_folder = output_folder
        else:
            self.output_folder = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[:-3]   

        if output_file is not None:
            self.output_file = output_file
        else:
            self.output_file = "collection.json"
        
        if not os.path.exists(self.output_folder):
            os.mkdir(self.output_folder)

        self.stac_collection = None

        self.verbose = verbose

        self.bucket_name = bucket_name
        self.bucket_file_prefix = bucket_file_prefix

        # Initializing an S3 client
        self.s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key) #region_name=aws_region, 


    def set_media_type(self, media_type: pystac.MediaType):
        self.media_type = media_type

    # TODO/FIXME: maybe better to put this method as an external static function? (and s3_client attribute as global variable) 
    def upload_s3(self, file_path):
        #Getting object name adding prefix + '/' + local_file_name
        prefix = self.bucket_file_prefix
        file_name = os.path.basename(file_path)
        object_name = f"{prefix if prefix.endswith('/') else prefix + '/'}{file_name}"

        try:
            self.s3_client.upload_file(file_path, self.bucket_name, object_name)

            if self.verbose: 
                _log.debug(f'Successfully uploaded {file_name} to {self.bucket_name} as {object_name}')
        except botocore.exceptions.NoCredentialsError:
            if self.verbose:
                _log.debug('AWS credentials not found. Make sure you set the correct access key and secret key.')
        except botocore.exceptions.ClientError as e:
            if self.verbose:
                _log.debug(f'Error uploading file: {e.response["Error"]["Message"]}')

    def generate_stac(self):

        spatial_extents = []
        temporal_extents = []

        item_list = []  # Create a list to store the items

        # Get the time dimension values
        time_values = self.data[self.t_dim].values
        
        #Cycling all timestamps

        if self.verbose:
            _log.debug("Cycling all timestamps")

        for t in time_values:
            if self.verbose:
                _log.debug(f"\nts: {t}")

            # Convert the time value to a datetime object
            timestamp = pd.Timestamp(t)

            # Format the timestamp as a string to use in the file name
            time_str = timestamp.strftime('%Y%m%d%H%M%S')

            # Create a unique directory for each time slice
            time_slice_dir = os.path.join(self.output_folder, time_str)

            if not os.path.exists(time_slice_dir):
                os.makedirs(time_slice_dir)

            # Get the band name (you may need to adjust this part based on your data)
            bands = self.data[self.b_dim].values
            
            pystac_assets = []

            # Cycling all bands
            if self.verbose:
                _log.debug("Cycling all bands")

            for band in bands:
                if self.verbose:
                    _log.debug(f"b: {band}")

                # Define the GeoTIFF file path for this time slice and band
                path = os.path.join(time_slice_dir, f"{band}_{time_str}.tif")

                # Write the result to the GeoTIFF file
                self.data.loc[{self.t_dim:t,self.b_dim:band}].rio.to_raster(raster_path=path, driver='COG')

                #Uploading file to s3
                _log.debug(f"Uploading {path} to {self.bucket_file_prefix if self.bucket_file_prefix.endswith('/') else self.bucket_file_prefix + '/'}{os.path.basename(path)}")
                self.upload_s3(path)

                
                bboxes = []

                # Create an asset dictionary for this time slice
                with rasterio.open(path) as src_dst:
                    # Get BBOX and Footprint
                    dataset_geom = get_dataset_geom(src_dst, densify_pts=0, precision=-1)
                    bboxes.append(dataset_geom["bbox"])

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
                    # TODO: try to add this field to the COG. Currently not present in the files we write here.
                    if cloudcover is not None:
                        self.properties.update({"eo:cloud_cover": int(cloudcover)})

                    pystac_assets.append(
                        (
                            band, 
                            pystac.Asset(
                                href=path, 
                                media_type=self.media_type,
                                extra_fields={
                                    **proj_info,
                                    **raster_info, 
                                    **eo_info
                                },
                                roles=None,
                            ),
                        )
                    )
            

            minx, miny, maxx, maxy = zip(*bboxes)
            bbox = [min(minx), min(miny), max(maxx), max(maxy)]

            # metadata_item_path = f"{time_slice_dir}/metadata.json"

            # item
            item = pystac.Item(
                id=time_str,
                geometry=bbox_to_geom(bbox),
                bbox=bbox,
                collection=None,#self.collection_id,
                stac_extensions=self.extensions,
                datetime=str_to_datetime(str(t)),
                properties=self.properties,
                # href=metadata_item_path # no more needed after removing JSON for every item approach 
            )

            # Calculate the item's spatial extent and add it to the list
            item_bbox = item.bbox
            spatial_extents.append(item_bbox)

            # Calculate the item's temporal extent and add it to the list
            item_datetime = item.datetime
            temporal_extents.append([item_datetime, item_datetime])


            for key, asset in pystac_assets:
                item.add_asset(key=key, asset=asset)
            
            """
            # produce single metatada for all items if specified by flag
            json_str = (json.dumps(item.to_dict(), indent=4))
            #printing metadata.json test output file
            with open(metadata_item_path, "w+") as metadata:
                metadata.write(json_str)
            """
            
            item.validate()
            

            #if we add a collection we MUST add a link
            if self.collection_id:
                item.add_link(
                    pystac.Link(
                        pystac.RelType.COLLECTION,
                        self.collection_url or self.collection_id,
                        media_type=pystac.MediaType.JSON,
                    )
                )
            
            # self.stac_collection.add_item(item)
            
            # Append the item to the list instead of adding it to the collection
            item_dict = item.to_dict()
            item_list.append(copy.deepcopy(item_dict)) # If we don't get a deep copy, the properties datetime gets overwritten in the next iteration of the loop, don't know why.


        
        # Calculate overall spatial extent
        minx, miny, maxx, maxy = zip(*spatial_extents)
        overall_bbox = [min(minx), min(miny), max(maxx), max(maxy)]

        # Calculate overall temporal extent
        min_datetime = min(temporal_extents, key=lambda x: x[0])[0]
        max_datetime = max(temporal_extents, key=lambda x: x[1])[1]
        overall_temporal_extent = [min_datetime, max_datetime]

        s_ext = pystac.SpatialExtent([overall_bbox])
        t_ext = pystac.TemporalExtent([overall_temporal_extent])

        self.stac_collection = pystac.collection.Collection(
            id=self.collection_id,
            description=self.description,
            extent=pystac.Extent(spatial=s_ext, temporal=t_ext),
            extra_fields={"stac_version": self.stac_version},
            
        )

        # Create a single JSON file with all the items
        stac_collection_dict = self.stac_collection.to_dict()
        stac_collection_dict["features"] = item_list  # Replace the "features" field with the list of items

        json_str = json.dumps(stac_collection_dict, indent=4)
        
        #printing metadata.json test output file
        output_path = Path(self.output_folder) / Path(self.output_file)
        with open(output_path, "w+") as metadata:
            metadata.write(json_str)

        #Uploading metadata JSON file to s3
        _log.debug(f"Uploading metatada JSON \"{output_path}\" to {self.bucket_file_prefix if self.bucket_file_prefix.endswith('/') else self.bucket_file_prefix + '/'}{os.path.basename(output_path)}")
        self.upload_s3(output_path)
