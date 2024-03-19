"""
Raster2STAC - Extract STAC format metadata from raster data 

This module provides a class `Raster2STAC` for extracting from raster data, represented as an `xr.DataArray`
or a file path which links to a local .nc file (that will be converted in `xr.DataArray`), 
SpatioTemporal Asset Catalog (STAC) format metadata JSON files.
This allows the output data to be ingested into Eurac's STAC FastApi

Authors: Mercurio Lorenzo, Eurac Research - Inst. for Earth Observation, Bolzano/Bozen IT
Authors: Michele Claus, Eurac Research - Inst. for Earth Observation, Bolzano/Bozen IT
Date: 2024-03-06
"""
import sys
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
import numpy as np
import json
import ujson
import xarray as xr
from typing import Callable, Optional, Union
import logging
import boto3
import botocore
import dask
import openeo_processes_dask
from urllib.parse import urlparse, urlunparse
from fsspec.implementations.local import LocalFileSystem
import openeo_processes_dask.process_implementations.cubes._xr_interop
from openeo.local import LocalConnection

from .rioxarray_stac import rioxarray_get_dataset_geom, rioxarray_get_projection_info, rioxarray_get_raster_info

_log = logging.getLogger(__name__)


DATACUBE_EXT_VERSION = "v1.0.0"

class Raster2STAC():
    """
    Raster2STAC Class - Converte dati raster nel formato STAC.

    Args:
        data: str or xr.DataArray
            Raster data as xr.DataArray or file path referring to a netCDF file.
        collection_id: str
            Identifier of the collection as a string (Example: 'blue-river-basin')
        collection_url: str
            Collection URL (must refer to the FastAPI URL where this collection will be uploaded).
        item_prefix: Optional[str] = ""
            Prefix to add before the datetime as item_id, if the same datetime can occur in multiple items.
        output_folder: Optional[str] = None
            Local folder for rasters and STAC metadata outputs. Default folder will be set as run timestamp folder 
            (ex: ./20231215103000/)
        description: Optional[str] = ""
            Description of the STAC collection.
        title: Optional[str] = None,
            Title of the STAC collection.
        ignore_warns: Optional[bool] = False,
            If True, warnings during processing (such as xr lib warnings) will be ignored.
        keywords: Optional[list] = None,
            Keywords associated with the STAC item.
        providers: Optional[list] = None,
            Data providers associated with the STAC item.
        stac_version: str = "1.0.0",
            Version of the STAC specification to use.
        s3_upload: bool = True,
            If True, upload files to Amazon S3 Bucket.
            1. For the "COG" output format: upload to S3 the COG files
            2. For the "KERCHUNK" output format: upload the netCDFs and the json Kerchunk files to S3.
        bucket_name: str = None,
            Part of AWS S3 configuration: bucket name.
        bucket_file_prefix: str = None,
             Part of AWS S3 configuration: prefix for files in the S3 bucket.
        aws_region: str = None,
             Part of AWS S3 configuration: AWS region for S3.
        version: Optional[str] = None,
            Version of the STAC collection.
        license: Optional[str] = None,
            License information about STAC collection and its assets.
        sci_citation: Optional[str] = None
            Scientific citation(s) reference(s) about STAC collection.
    """



    def __init__(self,
                 data,
                 collection_id,
                 collection_url = None,
                 item_prefix: Optional[str] = "",
                 output_folder: Optional[str] = None,
                 description: Optional[str] = "",
                 title: Optional[str] = None,
                 ignore_warns: Optional[bool] = False,
                 keywords: Optional[list] = None,
                 providers: Optional[list] = None,
                 links: Optional[list] = None,
                 stac_version="1.0.0",
                 s3_upload=False,
                 bucket_name = None,
                 bucket_file_prefix = None,
                 aws_region = None,
                 version = None,
                 license = None,
                 sci_doi = None,
                 sci_citation=None
                ):
        
        if ignore_warns == True:
            import warnings
            warnings.filterwarnings("ignore")

        self.data = data
        self.X_DIM = None
        self.Y_DIM = None
        self.T_DIM = None
        self.B_DIM = None
        self.output_format = None
        self.media_type = None

        self.properties = {} # additional properties to add in the item
        self.collection_id = collection_id # name of collection the item belongs to
        self.collection_url = collection_url
        self.item_prefix = item_prefix
        self.description = description
        self.keywords = keywords
        self.providers = providers
        self.links = links
        self.stac_version = stac_version
        self.sci_doi = sci_doi
        self.extensions = [
            f"https://stac-extensions.github.io/projection/{PROJECTION_EXT_VERSION}/schema.json", 
            f"https://stac-extensions.github.io/raster/{RASTER_EXT_VERSION}/schema.json",
            f"https://stac-extensions.github.io/eo/{EO_EXT_VERSION}/schema.json",
            # f"https://stac-extensions.github.io/datacube/{DATACUBE_EXT_VERSION}/schema.json",
        ]

        if output_folder is not None:
            self.output_folder = output_folder
        else:
            self.output_folder = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[:-3]

        self.output_file = f"{self.collection_id}.json"
        
        Path(self.output_folder).mkdir(parents=True, exist_ok=True)

        self.stac_collection = None
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
        # available_output_formats = ["csv","json_full"]
        # if output_format not in available_output_formats:
            # raise ValueError(f"output_format can be set to one of {available_output_formats}")
        # self.output_format = output_format
        self.license = license
        self.sci_citation = sci_citation

        #TODO: implement following attributes: self.overwrite, 

    def fix_path_slash(self, res_loc):
        return res_loc if res_loc.endswith('/') else res_loc + '/'
        

    # TODO/FIXME: maybe better to put this method as an external static function? (and s3_client attribute as global variable) 
    def upload_s3(self, file_path):
        if self.s3_client is not None:
            prefix = self.bucket_file_prefix
            file_name = os.path.basename(file_path)
            object_name = f"{self.fix_path_slash(prefix)}{file_name}"

            try:
                self.s3_client.upload_file(file_path, self.bucket_name, object_name)
                _log.debug(f'Successfully uploaded {file_name} to {self.bucket_name} as {object_name}')
            except botocore.exceptions.NoCredentialsError:
                _log.debug('AWS credentials not found. Make sure you set the correct access key and secret key.')
            except botocore.exceptions.ClientError as e:
                _log.debug(f'Error uploading file: {e.response["Error"]["Message"]}')


    def get_root_url(self, url):
        parsed_url = urlparse(url)
        # Extract protocol + domain
        root_url = urlunparse((parsed_url.scheme, parsed_url.netloc, '', '', '', ''))
        return root_url

    def generate_kerchunk_stac(self):
        from kerchunk.hdf import SingleHdf5ToZarr
        def gen_json(u, so, json_dir):
            fs = LocalFileSystem(skip_instant_cache=True)
            with fs.open(u, **so) as inf:
                h5chunks = SingleHdf5ToZarr(inf, u, inline_threshold=300)
                with open(f"{str(json_dir)}/{u.split('/')[-1]}.json", 'wb') as outf:
                    outf.write(ujson.dumps(h5chunks.translate()).encode())
            return f"{str(json_dir)}/{u.split('/')[-1]}.json"

        # Create the output folder for the Kerchunk files
        kerchunk_folder = os.path.join(self.output_folder,"kerchunk")
        Path(kerchunk_folder).mkdir(parents=True, exist_ok=True)
        
        kerchunk_files_list = []
        # Read the list of netCDFs
        for same_time_netcdfs in self.data:
            t_labels = []
            for var in same_time_netcdfs:
                source_nc = var 
                source_path = os.path.dirname(var)
                local_conn = LocalConnection(source_path)
                tmp = local_conn.load_collection(source_nc).execute()
                t_labels.append(tuple(tmp[tmp.openeo.temporal_dims[0]].values))
            t_steps = [len(x) for x in t_labels]
            if len(set(t_steps)) != 1:
                raise ValueError(f"The provided netCDFs contain a different number of dates! {same_time_netcdfs}")
            if len(set(t_labels)) != 1:
                raise ValueError(f"The provided netCDFs contain a different set of dates!")

            so = dict(mode='rb', anon=True, default_fill_cache=False)
            same_time_kerchunks = dask.compute(*[dask.delayed(gen_json)(var, so, kerchunk_folder) for var in same_time_netcdfs])
            kerchunk_files_list.append(same_time_kerchunks)
            
        
        # List of List json Kerchunk.
        # First list: each element (list) different year/time
        # Second list: each element different variables
        datasets_list = []
        for same_time_data in kerchunk_files_list:
            for d in same_time_data:
                if d.endswith(".json"):
                    self.data = xr.open_dataset(
                        "reference://",
                        engine="zarr",
                        decode_coords="all",
                        backend_kwargs={
                            "storage_options": {
                                "fo":d,
                            },
                            "consolidated": False
                        },chunks={}
                    ).to_dataarray(dim="bands")
                    IS_KERCHUNK = True
                    datasets_list.append(self.data)
                    # Need to create one Item per time/netCDF
        self.data = xr.combine_by_coords(datasets_list,combine_attrs="drop_conflicts")
        # raise ValueError("'data' paramter must be either xr.DataArray, a str (path to a netCDF) or a list of lists with paths to JSON Kerchunk files.") 

        self.X_DIM = self.data.openeo.x_dim
        self.Y_DIM = self.data.openeo.y_dim
        self.T_DIM = self.data.openeo.temporal_dims[0]
        self.B_DIM = self.data.openeo.band_dims[0]
        _log.debug(f'Extracted label dimensions from input are:\nx dimension:{self.X_DIM}\ny dimension:{self.Y_DIM}\nbands dimension:{self.B_DIM}\ntemporal dimension:{self.T_DIM}')
                
        self.output_format = "KERCHUNK"
        self.media_type = pystac.MediaType.JSON
        
        spatial_extents = []
        temporal_extents = []

        item_list = []  # Create a list to store the items

        # Get the time dimension values
        time_values = self.data[self.T_DIM].values

        eo_info = {}
        
        #resetting CSV file
        open(f"{Path(self.output_folder)}/inline_items.csv", 'w+') 
        
        _log.debug("Cycling all timestamps")

        # Loop over the kerchunk files
        for same_time_data in kerchunk_files_list:
            _log.debug(f"\nts: {same_time_data}")
                
            time_ranges = []
            bands_data = {}
            for d in same_time_data:
                if d.endswith(".json"):
                    band_data = xr.open_dataset(
                            "reference://",
                            engine="zarr",
                            decode_coords="all",
                            backend_kwargs={
                            "storage_options": {
                                "fo":d,
                            },
                            "consolidated": False
                        },chunks={}
                    ).to_dataarray(dim="bands")
                    bands_data[d] = band_data
                    time_ranges.append(band_data[self.T_DIM].values)
            # for i,t_r in enumerate(time_ranges):
            #     if i==0:
            #         first_range = t_r
            #         _are_all_time_steps_equal = True
            #     else:
            #         _are_all_time_steps_equal = np.array_equal(first_range,t_r) and _are_all_time_steps_equal
                    
                    
#             _log.debug(f"Are the time steps provided in the kerchunks aligned {_are_all_time_steps_equal}")
            
#             if not _are_all_time_steps_equal:
#                 raise Exception(f"The time steps provided in the kerchunk files {same_time_data} are not the same, can't continue.")
            
            # Now we can create one STAC Item for this time range, with one asset each band/variable

            start_datetime = np.min(time_ranges[0])
            end_datetime = np.max(time_ranges[0])
            
            # Convert the time value to a datetime object
            # Format the timestamp as a string to use in the file name
            start_datetime_str = pd.Timestamp(start_datetime).strftime('%Y%m%d%H%M%S')
            end_datetime_str = pd.Timestamp(end_datetime).strftime('%Y%m%d%H%M%S')

            _log.debug(f"Extracted temporal extrema for this time range: {start_datetime_str} {end_datetime_str}")
          
            item_id = f"{f'{self.item_prefix}_' if self.item_prefix != '' else ''}{start_datetime_str}_{end_datetime_str}"

            # Create a unique directory for each time slice
            time_slice_dir = os.path.join(self.output_folder, f"{start_datetime_str}_{end_datetime_str}")

            Path(time_slice_dir).mkdir(parents=True, exist_ok=True)

            # Get the band name (you may need to adjust this part based on your data)
            # bands = self.data[self.B_DIM].values
            
            pystac_assets = []

            # Cycling all bands/variables
            _log.debug("Cycling all bands")

            eo_bands_list = []
            for b_d in bands_data:
                band = bands_data[b_d][self.B_DIM].values[0]
                kerchunk_file = b_d
                _log.debug(f"b: {band}")

                link_path = kerchunk_file 

#                 if self.s3_upload:                                     
#                     #Uploading file to s3                   
#                     _log.debug(f"Uploading {path} to {self.fix_path_slash(self.bucket_file_prefix)}{os.path.basename(path)}")
#                     self.upload_s3(path)
                    
#                     link_path = f"https://{self.bucket_name}.{self.aws_region}.amazonaws.com/{self.fix_path_slash(self.bucket_file_prefix)}{curr_file_name}"

                bboxes = []

                # Create an asset dictionary for this time slice
                # Get BBOX and Footprint
                _log.debug(bands_data[b_d].rio.crs)
                _log.debug(bands_data[b_d].rio.bounds())

                dataset_geom = rioxarray_get_dataset_geom(bands_data[b_d], densify_pts=0, precision=-1)
                bboxes.append(dataset_geom["bbox"])
                
                proj_info = {
                    f"proj:{name}": value
                    for name, value in rioxarray_get_projection_info(bands_data[b_d]).items()
                }

                raster_info = {
                    "raster:bands": rioxarray_get_raster_info(bands_data[b_d], max_size=1024)
                }

                band_dict = {
                    "name": band
                    }

                eo_bands_list.append(band_dict)

                eo_info["eo:bands"] = [band_dict]

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
                            roles=["data","index"],
                        ),
                    )
                )
                
            
            eo_info["eo:bands"] = eo_bands_list

            minx, miny, maxx, maxy = zip(*bboxes)
            bbox = [min(minx), min(miny), max(maxx), max(maxy)]

            # item
            item = pystac.Item(
                id=item_id,
                geometry=bbox_to_geom(bbox),
                bbox=bbox,
                collection=None, #self.collection_id, #FIXME: da errore se lo si decommenta
                stac_extensions=self.extensions,
                datetime=None,
                start_datetime=pd.Timestamp(start_datetime),
                end_datetime=pd.Timestamp(end_datetime),
                properties=self.properties,
            )

            # Calculate the item's spatial extent and add it to the list
            spatial_extents.append(item.bbox)

            # Calculate the item's temporal extent and add it to the list
            # item_datetime = item.start_datetime
            temporal_extents.append([pd.Timestamp(start_datetime), pd.Timestamp(end_datetime)])

            for key, asset in pystac_assets:
                item.add_asset(key=key, asset=asset)
            
            item.validate()
            
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
                    f"{self.fix_path_slash(self.collection_url)}{self.collection_id}/{item_id}",
                    media_type=pystac.MediaType.JSON,
                )
            )

            item_dict = item.to_dict()

            #FIXME: persistent pystac bug or logical error (urllib error when adding root link to current item)
            # now this link is added manually by editing the dict
            item_dict["links"].append({"rel": "root", "href": self.get_root_url(f"{self.fix_path_slash(self.collection_url)}{self.collection_id}"), "type": "application/json"})

            
            # self.stac_collection.add_item(item)
            # Append the item to the list instead of adding it to the collection
            #item_dict = item.to_dict()
            item_dict["collection"] = self.collection_id

            # if self.output_format == "json_full":
            # item_list.append(copy.deepcopy(item_dict)) # If we don't get a deep copy, the properties datetime gets overwritten in the next iteration of the loop, don't know why.
            # elif self.output_format == "csv":
            item_oneline = json.dumps(item_dict, separators=(",", ":"), ensure_ascii=False)

            output_path = Path(self.output_folder)
            with open(f"{output_path}/inline_items.csv", 'a+') as out_csv:
                out_csv.write(f"{item_oneline}\n")


            jsons_path = f"{output_path}/items/"
            Path(jsons_path).mkdir(parents=True, exist_ok=True)

            with open(f"{self.fix_path_slash(jsons_path)}{item_id}.json", 'w+') as out_json:
                out_json.write(json.dumps(item_dict, indent=4))            
        
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
        
        if self.sci_doi is not None:
            extra_fields["sci:doi"] = self.sci_doi
        
        if self.sci_citation is not None or self.sci_doi is not None:
            self.extensions.append("https://stac-extensions.github.io/scientific/v1.0.0/schema.json")
    

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
        
        if self.links is not None:
            stac_collection_dict["links"] = stac_collection_dict["links"] + self.links

        # if self.output_format == "json_full":       
            # stac_collection_dict["features"] = item_list  # Replace the "features" field with the list of items

        json_str = json.dumps(stac_collection_dict, indent=4)

        #printing metadata.json test output file
        output_path = Path(self.output_folder) / Path(self.output_file)
        with open(output_path, "w+") as metadata:
            metadata.write(json_str)

        if self.s3_upload:
            #Uploading metadata JSON file to s3
            _log.debug(f"Uploading metatada JSON \"{output_path}\" to {self.fix_path_slash(self.bucket_file_prefix)}{os.path.basename(output_path)}")
            self.upload_s3(output_path)

    def generate_cog_stac(self):
        if isinstance(self.data, xr.DataArray) or isinstance(self.data, str):
            if(isinstance(self.data, xr.DataArray)):
                pass
            elif(isinstance(self.data, str)):
                source_path = os.path.dirname(self.data)
                local_conn = LocalConnection(source_path)
                self.data = local_conn.load_collection(self.data).execute()

        self.output_format = "COG"
        self.media_type = pystac.MediaType.COG  # we could also use rio_stac.stac.get_media_type)
        self.X_DIM = self.data.openeo.x_dim
        self.Y_DIM = self.data.openeo.y_dim
        self.T_DIM = self.data.openeo.temporal_dims[0]
        self.B_DIM = self.data.openeo.band_dims[0]
        _log.debug(f'Extracted label dimensions from input are:\nx dimension:{self.X_DIM}\ny dimension:{self.Y_DIM}\nbands dimension:{self.B_DIM}\ntemporal dimension:{self.T_DIM}')

        spatial_extents = []
        temporal_extents = []

        item_list = []  # Create a list to store the items

        # Get the time dimension values
        time_values = self.data[self.T_DIM].values
        
        eo_info = {}
        
        #resetting CSV file
        open(f"{Path(self.output_folder)}/inline_items.csv", 'w+') 
        
        _log.debug("Cycling all timestamps")

        #Cycling all timestamps
        for t in time_values:
            _log.debug(f"\nts: {t}")

            # Convert the time value to a datetime object
            timestamp = pd.Timestamp(t)

            # Format the timestamp as a string to use in the file name
            time_str = timestamp.strftime('%Y%m%d%H%M%S')

            item_id = f"{f'{self.item_prefix}_' if self.item_prefix != '' else ''}{time_str}"

            # Create a unique directory for each time slice
            time_slice_dir = os.path.join(self.output_folder, time_str)

            if not os.path.exists(time_slice_dir):
                os.makedirs(time_slice_dir)

            # Get the band name (you may need to adjust this part based on your data)
            bands = self.data[self.B_DIM].values
            
            pystac_assets = []

            # Cycling all bands
            _log.debug("Cycling all bands")

            eo_bands_list = []

            for band in bands:
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

                    eo_info["eo:bands"] = [band_dict]

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
                                roles=["data"],
                            ),
                        )
                    )
            
            eo_info["eo:bands"] = eo_bands_list

            minx, miny, maxx, maxy = zip(*bboxes)
            bbox = [min(minx), min(miny), max(maxx), max(maxy)]

            # metadata_item_path = f"{self.fix_path_slash(time_slice_dir)}metadata.json"

            # item
            item = pystac.Item(
                id=item_id,
                geometry=bbox_to_geom(bbox),
                bbox=bbox,
                collection=None, #self.collection_id, #FIXME: da errore se lo si decommenta
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
                    f"{self.fix_path_slash(self.collection_url)}{self.collection_id}/items/{item_id}",
                    media_type=pystac.MediaType.JSON,
                )
            )

            item_dict = item.to_dict()

            #FIXME: persistent pystac bug or logical error (urllib error when adding root link to current item)
            # now this link is added manually by editing the dict
            item_dict["links"].append({"rel": "root", "href": self.get_root_url(f"{self.fix_path_slash(self.collection_url)}{self.collection_id}"), "type": "application/json"})

            
            # self.stac_collection.add_item(item)
            # Append the item to the list instead of adding it to the collection
            #item_dict = item.to_dict()
            item_dict["collection"] = self.collection_id

            # if self.output_format == "json_full":
                # item_list.append(copy.deepcopy(item_dict)) # If we don't get a deep copy, the properties datetime gets overwritten in the next iteration of the loop.
            # elif self.output_format == "csv":
            item_oneline = json.dumps(item_dict, separators=(",", ":"), ensure_ascii=False)

            output_path = Path(self.output_folder)
            with open(f"{output_path}/inline_items.csv", 'a+') as out_csv:
                out_csv.write(f"{item_oneline}\n")


            jsons_path = f"{output_path}/items/"
            Path(jsons_path).mkdir(parents=True, exist_ok=True)

            with open(f"{self.fix_path_slash(jsons_path)}{item_id}.json", 'w+') as out_json:
                out_json.write(json.dumps(item_dict, indent=4))

        
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
        
        if self.sci_doi is not None:
            extra_fields["sci:doi"] = self.sci_doi
        
        if self.sci_citation is not None or self.sci_doi is not None:
            self.extensions.append("https://stac-extensions.github.io/scientific/v1.0.0/schema.json")

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
        
        if self.links is not None:
            stac_collection_dict["links"] = stac_collection_dict["links"] + self.links

        # if self.output_format == "json_full":       
            # stac_collection_dict["features"] = item_list  # Replace the "features" field with the list of items

        json_str = json.dumps(stac_collection_dict, indent=4)

        #printing metadata.json test output file
        output_path = Path(self.output_folder) / Path(self.output_file)
        with open(output_path, "w+") as metadata:
            metadata.write(json_str)

        if self.s3_upload:
            #Uploading metadata JSON file to s3
            _log.debug(f"Uploading metatada JSON \"{output_path}\" to {self.fix_path_slash(self.bucket_file_prefix)}{os.path.basename(output_path)}")
            self.upload_s3(output_path)
