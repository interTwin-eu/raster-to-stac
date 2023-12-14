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
import openeo_processes_dask
from urllib.parse import urlparse, urlunparse


_log = logging.getLogger(__name__)
#_log.setLevel(logging.INFO)

conf_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'conf.json')

# loading confs from conf.json
with open(conf_path, 'r') as f:
    conf_data = json.load(f)

# loadng AWS credentials/confs
aws_access_key = conf_data["s3"]["aws_access_key"]
aws_secret_key = conf_data["s3"]["aws_secret_key"]
#aws_region    = conf_data["s3"]["aws_region"]

class Raster2STAC():
    
    def __init__(self,
                 #data: xr.DataArray,
                 data,
                 collection_id: Optional[str] = None,         #collection id as string (same of collection and items)
                 collection_url: Optional[str] = None,
                 output_folder: Optional[str] = None,
                 output_file: Optional[str] = None,
                 description: Optional[str] = "",
                 title: Optional[str] = None,

                 keywords: Optional[list] = None,  ### down below: if None, don't put that key on the structure
                 providers: Optional[list] = None,  ### down below: if None, don't put that key on the structure

                 stac_version="1.0.0",
                 verbose=False,
                 s3_upload=True,
                 bucket_name = conf_data["s3"]["bucket_name"],
                 bucket_file_prefix = conf_data["s3"]["bucket_file_prefix"],
                 aws_region = conf_data["s3"]["aws_region"],
                 version = None,
                 output_format="json_full",
                 license = None,
                 write_json_items = False,
                 sci_citation=None
                ):
        
        if isinstance(data, xr.DataArray) or isinstance(data, str):
            if(isinstance(data, xr.DataArray)):
                self.data = data
            elif(isinstance(data, str)):
                from openeo.local import LocalConnection
                source_nc = data 
                source_path = os.path.dirname(data)
                local_conn = LocalConnection(source_path)
                s2_datacube = local_conn.load_collection(source_nc)
                self.data = s2_datacube.execute()
        else:
            raise ValueError("'data' paramter must be either xr.DataArray or str")


        self.X_DIM = self.data.openeo.x_dim
        self.Y_DIM = self.data.openeo.y_dim
        self.T_DIM = self.data.openeo.temporal_dims[0]
        self.B_DIM = self.data.openeo.band_dims[0]

        
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

        self.keywords = keywords

        self.providers = providers

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

        self.aws_region = aws_region

        self.s3_upload = s3_upload

        self.s3_client = None

        self.version = version 

        self.title = title

        if self.s3_upload:
            # Initializing an S3 client
            self.s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key) #region_name=aws_region, 

        self.output_format = output_format

        # if self.output_format not in[ "json_full", "csv" ]:
        #    self.s3_upload = False

        self.license = license

        self.write_json_items = write_json_items

        self.sci_citation = sci_citation

    def fix_path_slash(self, res_loc):
        return res_loc if res_loc.endswith('/') else res_loc + '/'


    def set_media_type(self, media_type: pystac.MediaType):
        self.media_type = media_type

    # TODO/FIXME: maybe better to put this method as an external static function? (and s3_client attribute as global variable) 
    def upload_s3(self, file_path):
        if self.s3_client is not None:
            prefix = self.bucket_file_prefix
            file_name = os.path.basename(file_path)
            object_name = f"{self.fix_path_slash(prefix)}{file_name}"

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


    def get_root_url(self, url):
        parsed_url = urlparse(url)
        # Extract protocol + domain
        root_url = urlunparse((parsed_url.scheme, parsed_url.netloc, '', '', '', ''))
        return root_url

    def generate_stac(self):
        spatial_extents = []
        temporal_extents = []

        item_list = []  # Create a list to store the items

        # Get the time dimension values
        time_values = self.data[self.T_DIM].values
        
        #Cycling all timestamps

        if self.verbose:
            _log.debug("Cycling all timestamps")

        eo_info = {}

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
            bands = self.data[self.B_DIM].values
            
            pystac_assets = []

            # Cycling all bands
            if self.verbose:
                _log.debug("Cycling all bands")

            eo_bands_list = []

            for band in bands:
                if self.verbose:
                    _log.debug(f"b: {band}")

                curr_file_name = f"{band}_{time_str}.tif"
                # Define the GeoTIFF file path for this time slice and band
                path = os.path.join(time_slice_dir, curr_file_name)

                # Write the result to the GeoTIFF file
                self.data.loc[{self.T_DIM:t,self.B_DIM:band}].to_dataset(name=band).rio.to_raster(raster_path=path, driver='COG')

                link_path = path 

                if self.s3_upload:                                     
                    #Uploading file to s3                   
                    _log.debug(f"Uploading {path} to {self.fix_path_slash(self.bucket_file_prefix)}{os.path.basename(path)}")
                    self.upload_s3(path)
                    
                    link_path = f"https://{self.bucket_name}.{self.aws_region}.amazonaws.com/{self.fix_path_slash(self.bucket_file_prefix)}{curr_file_name}"

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

                    band_dict = get_eobands_info(src_dst)[0]

                    if type(band_dict) == dict:
                        del(band_dict["name"])
                        band_dict["name"] = band_dict["description"]
                        del(band_dict["description"])
                    else:
                        pass #band_dict = {}

                    eo_bands_list.append(band_dict) #TODO: add to dict, rename description with name and remove name 
                    
                    cloudcover = src_dst.get_tag_item("CLOUDCOVER", "IMAGERY")
                    # TODO: try to add this field to the COG. Currently not present in the files we write here.
                    if cloudcover is not None:
                        self.properties.update({"eo:cloud_cover": int(cloudcover)})

                    pystac_assets.append(
                        (
                            band, 
                            pystac.Asset(
                                href=link_path, 
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
            
            eo_info["eo:bands"] = eo_bands_list

            minx, miny, maxx, maxy = zip(*bboxes)
            bbox = [min(minx), min(miny), max(maxx), max(maxy)]

            # metadata_item_path = f"{self.fix_path_slash(time_slice_dir)}metadata.json"

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
            
            item.validate()
            
            item_dict = item.to_dict() #FIXME: declared and assigned now for root issue in item link (see below)

            #if we add a collection we MUST add a link
            if self.collection_id and self.collection_url: 

               
                item.add_link(
                    pystac.Link(
                        pystac.RelType.COLLECTION,
                        f"{self.fix_path_slash(self.collection_url)}{self.collection_id}",
                        media_type=pystac.MediaType.JSON,
                    )
                )
                
                item.add_link(
                    pystac.Link(
                        pystac.RelType.PARENT,
                        f"{self.fix_path_slash(self.collection_url)}{self.collection_id}",
                        media_type=pystac.MediaType.JSON,
                    )
                )
                
               
                
                item.add_link(
                    pystac.Link(
                        pystac.RelType.SELF,
                        f"{self.fix_path_slash(self.collection_url)}{self.collection_id}/{time_str}",
                        media_type=pystac.MediaType.JSON,
                    )
                )

                item_dict = item.to_dict()

                #FIXME: persistent pystac bug or logical error (urllib error when adding root link to current item)
                # now this link is added manually by editing the dict
                item_dict["links"].append({"rel": "root", "href": self.get_root_url(f"{self.fix_path_slash(self.collection_url)}{self.collection_id}"), "type": "application/json"})


                """item.add_link(
                    pystac.Link(
                        pystac.RelType.ROOT,
                        #f"{self.fix_path_slash(self.collection_url)}{self.collection_id}",

                        self.get_root_url(f"{self.fix_path_slash(self.collection_url)}{self.collection_id}"),
                        media_type=pystac.MediaType.JSON,
                    )
                ) """              
                
            # self.stac_collection.add_item(item)
            
            # Append the item to the list instead of adding it to the collection
            #item_dict = item.to_dict()
            item_dict["collection"] = self.collection_id #self.title

            if self.output_format == "json_full":
                item_list.append(copy.deepcopy(item_dict)) # If we don't get a deep copy, the properties datetime gets overwritten in the next iteration of the loop, don't know why.
            elif self.output_format == "csv":
                item_oneline = json.dumps(item_dict, separators=(",", ":"), ensure_ascii=False)

                output_path = Path(self.output_folder)

                with open(f"{output_path}/items.csv", 'a+') as out_csv:
                    out_csv.write(f"{item_oneline}\n")

                if self.write_json_items:
                    jsons_path = f"{output_path}/items-json/"
                    if not os.path.exists(jsons_path):
                        os.mkdir(jsons_path)

                    with open(f"{self.fix_path_slash(jsons_path)}{self.collection_id}-{time_str}.json", 'w+') as out_json:
                        out_json.write(json.dumps(item_dict, indent=4))
            else:
                pass # TODO: implement further formats here
            
        
        # Calculate overall spatial extent
        minx, miny, maxx, maxy = zip(*spatial_extents)
        overall_bbox = [min(minx), min(miny), max(maxx), max(maxy)]

        # Calculate overall temporal extent
        min_datetime = min(temporal_extents, key=lambda x: x[0])[0]
        max_datetime = max(temporal_extents, key=lambda x: x[1])[1]
        overall_temporal_extent = [min_datetime, max_datetime]

        s_ext = pystac.SpatialExtent([overall_bbox])
        t_ext = pystac.TemporalExtent([overall_temporal_extent])

        extra_fields = {}

        extra_fields["stac_version"] = self.stac_version

        if self.keywords is not None:
            extra_fields["keywords"] = self.keywords

        if self.providers is not None:
            extra_fields["providers"] = self.providers

        if self.version is not None:
            extra_fields["version"] = self.version

        if self.title is not None:
            extra_fields["title"] = self.title

        if self.sci_citation is not None:
            extra_fields["sci:citation"] = self.sci_citation

        extra_fields["summaries"] = eo_info

        extra_fields["stac_extensions"] = self.extensions

        cube_dimensons = {
            self.X_DIM: {
                "axis": "x",
                "type": "spatial",
                "extent": [float(self.data.coords[self.X_DIM].min()), float(self.data.coords[self.X_DIM].max())],
                "reference_system": int((self.data.rio.crs.to_string()).split(':')[1])
            },
            self.Y_DIM: {
                "axis": "y",
                "type": "spatial",
                "extent": [float(self.data.coords[self.Y_DIM].min()), float(self.data.coords[self.Y_DIM].max())],
                "reference_system": int((self.data.rio.crs.to_string()).split(':')[1])
            },

            self.T_DIM: {
                #"step": "P1D",
                "type": "temporal",
                "extent": [str(self.data[self.T_DIM].min().values), str(self.data[self.T_DIM].max().values)],
            },

            self.B_DIM: {
                "type": "bands",
                "values": list(self.data[self.B_DIM].values),
            }
        }

        extra_fields["cube:dimensions"] = cube_dimensons

        self.stac_collection = pystac.collection.Collection(
            id=self.collection_id,
            description=self.description,
            extent=pystac.Extent(spatial=s_ext, temporal=t_ext),
            extra_fields=extra_fields,
        )

        #if we add a collection we MUST add a link
        if self.collection_id and self.collection_url:
            self.stac_collection.add_link(
                pystac.Link(
                    pystac.RelType.ITEMS,
                    f"{self.fix_path_slash(self.collection_url)}{self.collection_id}/items",
                    media_type=pystac.MediaType.JSON,
                )
            )
            
            self.stac_collection.add_link(
                pystac.Link(
                    pystac.RelType.PARENT,
                    self.get_root_url(f"{self.fix_path_slash(self.collection_url)}{self.collection_id}/items"),
                    media_type=pystac.MediaType.JSON,
                )
            )

            self.stac_collection.add_link(
                 pystac.Link(
                    pystac.RelType.SELF,
                    f"{self.fix_path_slash(self.collection_url)}{self.collection_id}",
                    media_type=pystac.MediaType.JSON,
                )
            )

            #self.stac_collection.remove_links(rel=pystac.RelType.ROOT)

            self.stac_collection.add_link(
                pystac.Link(
                    pystac.RelType.ROOT,
                    self.get_root_url(f"{self.fix_path_slash(self.collection_url)}{self.collection_id}/items"),
                    media_type=pystac.MediaType.JSON,
                )
            )

            """
            self.stac_collection.add_links(
                [
                    pystac.Link(
                    pystac.RelType.ROOT,
                    self.get_root_url(f"{self.fix_path_slash(collection_url)}{self.collection_id}/items"),
                    media_type=pystac.MediaType.JSON,
                ),
                pystac.Link(
                    pystac.RelType.SELF,
                    f"{self.fix_path_slash(self.collection_url)}{self.collection_id}",
                    media_type=pystac.MediaType.JSON,
                ),
                 pystac.Link(
                    pystac.RelType.PARENT,
                    self.get_root_url(f"{self.fix_path_slash(self.collection_url)}{self.collection_id}/items"),
                    media_type=pystac.MediaType.JSON,
                ),
                 pystac.Link(
                    pystac.RelType.ITEMS,
                    f"{self.fix_path_slash(self.collection_url)}{self.collection_id}/items",
                    media_type=pystac.MediaType.JSON,
                )
                ]
            
            )
            """

        if self.license is not None:
            self.stac_collection.license = self.license

        # Create a single JSON file with all the items
        stac_collection_dict = self.stac_collection.to_dict()

        # in order to solve the double "root" link bug/issue       
        links_dict = stac_collection_dict["links"]

        ctr_roots = 0
        self_exists = False
        self_idx = 0

        for idx, link in enumerate(links_dict):
            if link["rel"] == "root":
                ctr_roots = ctr_roots + 1
            if link["rel"] == "self":
                self_exists = True
                self_idx = idx

        if ctr_roots == 2 and self_exists:
            for idx, link in enumerate(links_dict):
                if link["rel"] == "root" and link["href"] == links_dict[self_idx]["href"] and link["type"] == links_dict[self_idx]["type"]:
                    del links_dict[idx]
                    break
        
        
        if self.output_format == "json_full":       
            stac_collection_dict["features"] = item_list  # Replace the "features" field with the list of items

        json_str = json.dumps(stac_collection_dict, indent=4)
        
        #printing metadata.json test output file
        output_path = Path(self.output_folder) / Path(self.output_file)
        with open(output_path, "w+") as metadata:
            metadata.write(json_str)

        if self.s3_upload:
            #Uploading metadata JSON file to s3
            _log.debug(f"Uploading metatada JSON \"{output_path}\" to {self.fix_path_slash(self.bucket_file_prefix)}{os.path.basename(output_path)}")
            self.upload_s3(output_path)
