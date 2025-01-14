"""
Create STAC Item from a rasterio dataset.

Modified from https://github.com/developmentseed/rio-stac/blob/main/rio_stac/stac.py
Using as main data model an xArray object, accessing the properties using rioxarray instead of rasterio
"""

import datetime
import math
import os
import warnings
from contextlib import ExitStack
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy
import pystac
import rasterio
import xarray as xr
from pystac.utils import str_to_datetime
from rasterio import transform, warp
from rasterio.features import bounds as feature_bounds
from rasterio.io import DatasetReader, DatasetWriter, MemoryFile
from rasterio.vrt import WarpedVRT

PROJECTION_EXT_VERSION = "v1.1.0"
RASTER_EXT_VERSION = "v1.1.0"
EO_EXT_VERSION = "v1.1.0"

EPSG_4326 = rasterio.crs.CRS.from_epsg(4326)


def bbox_to_geom(bbox: Tuple[float, float, float, float]) -> Dict:
    """Return a geojson geometry from a bbox."""
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [bbox[0], bbox[1]],
                [bbox[2], bbox[1]],
                [bbox[2], bbox[3]],
                [bbox[0], bbox[3]],
                [bbox[0], bbox[1]],
            ]
        ],
    }


def rioxarray_get_dataset_geom(
    src_dst: xr.DataArray,
    densify_pts: int = 0,
    precision: int = -1,
) -> Dict:
    """Get Raster Footprint."""
    if densify_pts < 0:
        raise ValueError("`densify_pts` must be positive")

    if src_dst.rio.crs is not None:
        # 1. Create Polygon from raster bounds
        geom = bbox_to_geom(src_dst.rio.bounds())

        # 2. Densify the Polygon geometry
        if src_dst.rio.crs != EPSG_4326 and densify_pts:
            # Derived from code found at
            # https://stackoverflow.com/questions/64995977/generating-equidistance-points-along-the-boundary-of-a-polygon-but-cw-ccw
            coordinates = numpy.asarray(geom["coordinates"][0])

            densified_number = len(coordinates) * densify_pts
            existing_indices = numpy.arange(0, densified_number, densify_pts)
            interp_indices = numpy.arange(existing_indices[-1] + 1)
            interp_x = numpy.interp(interp_indices, existing_indices, coordinates[:, 0])
            interp_y = numpy.interp(interp_indices, existing_indices, coordinates[:, 1])
            geom = {
                "type": "Polygon",
                "coordinates": [[(x, y) for x, y in zip(interp_x, interp_y)]],
            }

        # 3. Reproject the geometry to "epsg:4326"
        geom = warp.transform_geom(
            src_dst.rio.crs, EPSG_4326, geom, precision=precision
        )
        bbox = feature_bounds(geom)

    else:
        warnings.warn(
            "Input file doesn't have CRS information, setting geometry and bbox to (-180,-90,180,90)."
        )
        bbox = (-180.0, -90.0, 180.0, 90.0)
        geom = bbox_to_geom(bbox)

    return {"bbox": list(bbox), "footprint": geom}


def rioxarray_get_projection_info(
    src_dst: xr.DataArray,
) -> Dict:
    """Get projection metadata.

    The STAC projection extension allows for three different ways to describe the coordinate reference system
    associated with a raster :
    - EPSG code
    - WKT2
    - PROJJSON

    All are optional, and they can be provided altogether as well. Therefore, as long as one can be obtained from
    the data, we add it to the returned dictionary.

    see: https://github.com/stac-extensions/projection

    """
    projjson = None
    wkt2 = None
    epsg = None
    if src_dst.rio.crs is not None:
        # EPSG
        epsg = src_dst.rio.crs.to_epsg() if src_dst.rio.crs.is_epsg_code else None

        # PROJJSON
        try:
            projjson = src_dst.rio.crs.to_dict(projjson=True)
        except (AttributeError, TypeError) as ex:
            warnings.warn(f"Could not get PROJJSON from dataset : {ex}")
            pass

        # WKT2
        try:
            wkt2 = src_dst.rio.crs.to_wkt()
        except Exception as ex:
            warnings.warn(f"Could not get WKT2 from dataset : {ex}")
            pass

    meta = {
        "epsg": epsg,
        "geometry": bbox_to_geom(src_dst.rio.bounds()),
        "bbox": list(src_dst.rio.bounds()),
        "shape": [src_dst.rio.height, src_dst.rio.width],
        "transform": list(src_dst.rio.transform()),
    }

    if projjson is not None:
        meta["projjson"] = projjson

    if wkt2 is not None:
        meta["wkt2"] = wkt2

    return meta


def get_eobands_info(
    src_dst: Union[DatasetReader, DatasetWriter, WarpedVRT, MemoryFile],
) -> List:
    """Get eo:bands metadata.

    see: https://github.com/stac-extensions/eo#item-properties-or-asset-fields

    """
    eo_bands = []

    colors = src_dst.colorinterp
    for ix in src_dst.indexes:
        band_meta = {"name": f"b{ix}"}

        descr = src_dst.descriptions[ix - 1]
        color = colors[ix - 1].name

        # Description metadata or Colorinterp or Nothing
        description = descr or color
        if description:
            band_meta["description"] = description

        eo_bands.append(band_meta)

    return eo_bands


def _rioxarray_get_stats(arr: numpy.ndarray, **kwargs: Any) -> Dict:
    """Calculate array statistics."""
    # Avoid non masked nan/inf values
    arr = arr.values
    numpy.ma.fix_invalid(arr, copy=False)
    sample, edges = numpy.histogram(arr)
    return {
        "statistics": {
            "mean": float(arr.mean()),
            "minimum": float(arr.min()),
            "maximum": float(arr.max()),
            "stddev": float(arr.std()),
            "valid_percent": float(numpy.count_nonzero(arr)) / float(arr.size) * 100,
        },
        "histogram": {
            "count": len(edges),
            "min": float(edges.min()),
            "max": float(edges.max()),
            "buckets": sample.tolist(),
        },
    }


def rioxarray_get_raster_info(  # noqa: C901
    src_dst: xr.DataArray,
    max_size: int = 1024,
) -> List[Dict]:
    """Get raster metadata.

    see: https://github.com/stac-extensions/raster#raster-band-object

    """
    height = src_dst.rio.height
    width = src_dst.rio.width
    if max_size:
        if max(width, height) > max_size:
            ratio = height / width
            if ratio > 1:
                height = max_size
                width = math.ceil(height / ratio)
            else:
                width = max_size
                height = math.ceil(width * ratio)

    meta: List[Dict] = []

    # area_or_point = src_dst.tags().get("AREA_OR_POINT", "").lower()

    # Missing `bits_per_sample` and `spatial_resolution`
    # It should contain only one band/variable
    # for band in src_dst.indexes:
    if src_dst.attrs.get("scale_factor",False):
        value = {
            "data_type": str(src_dst.dtype),
            "scale": src_dst.attrs["scale_factor"],
        }
    else:
        value = {
            "data_type": str(src_dst.dtype),
            "scale": 1,
        }

    # add offset
    if src_dst.attrs.get("add_offset",False):
        value["offset"] = src_dst.attrs["add_offset"]
    else:
        value["offset"] = 0
    # if area_or_point:
    #     value["sampling"] = area_or_point

    # If the Nodata is not set we don't forward it.
    if src_dst.rio.nodata is not None:
        if numpy.isnan(src_dst.rio.nodata):
            value["nodata"] = "nan"
        elif numpy.isposinf(src_dst.rio.nodata):
            value["nodata"] = "inf"
        elif numpy.isneginf(src_dst.rio.nodata):
            value["nodata"] = "-inf"
        else:
            value["nodata"] = (src_dst.rio.nodata).astype((src_dst.dtype)[:2])
    elif src_dst.attrs.get("_FillValue"):
        value["nodata"] = src_dst.attrs.get("_FillValue")

    # TODO: check if we can get the unit
    # if src_dst.rio.units[0] is not None:
    #     value["unit"] = src_dst.rio.units[0]

    value.update(_rioxarray_get_stats(src_dst))
    meta.append(value)

    return meta


def get_media_type(
    src_dst: Union[DatasetReader, DatasetWriter, WarpedVRT, MemoryFile],
) -> Optional[pystac.MediaType]:
    """Find MediaType for a raster dataset."""
    driver = src_dst.driver

    if driver == "GTiff":
        if src_dst.crs:
            return pystac.MediaType.GEOTIFF
        else:
            return pystac.MediaType.TIFF

    elif driver in [
        "JP2ECW",
        "JP2KAK",
        "JP2LURA",
        "JP2MrSID",
        "JP2OpenJPEG",
        "JPEG2000",
    ]:
        return pystac.MediaType.JPEG2000

    elif driver in ["HDF4", "HDF4Image"]:
        return pystac.MediaType.HDF

    elif driver in ["HDF5", "HDF5Image"]:
        return pystac.MediaType.HDF5

    elif driver == "JPEG":
        return pystac.MediaType.JPEG

    elif driver == "PNG":
        return pystac.MediaType.PNG

    warnings.warn("Could not determine the media type from GDAL driver.")
    return None


def create_stac_item(
    source: Union[str, DatasetReader, DatasetWriter, WarpedVRT, MemoryFile],
    input_datetime: Optional[datetime.datetime] = None,
    extensions: Optional[List[str]] = None,
    collection: Optional[str] = None,
    collection_url: Optional[str] = None,
    properties: Optional[Dict] = None,
    id: Optional[str] = None,
    assets: Optional[Dict[str, pystac.Asset]] = None,
    asset_name: str = "asset",
    asset_roles: Optional[List[str]] = None,
    asset_media_type: Optional[Union[str, pystac.MediaType]] = "auto",
    asset_href: Optional[str] = None,
    with_proj: bool = False,
    with_raster: bool = False,
    with_eo: bool = False,
    raster_max_size: int = 1024,
    geom_densify_pts: int = 0,
    geom_precision: int = -1,
) -> pystac.Item:
    """Create a Stac Item.

    Args:
        source (str or opened rasterio dataset): input path or rasterio dataset.
        input_datetime (datetime.datetime, optional): datetime associated with the item.
        extensions (list of str): input list of extensions to use in the item.
        collection (str, optional): name of collection the item belongs to.
        collection_url (str, optional): Link to the STAC Collection.
        properties (dict, optional): additional properties to add in the item.
        id (str, optional): id to assign to the item (default to the source basename).
        assets (dict, optional): Assets to set in the item. If set we won't create one from the source.
        asset_name (str, optional): asset name in the Assets object.
        asset_roles (list of str, optional): list of str | list of asset's roles.
        asset_media_type (str or pystac.MediaType, optional): asset's media type.
        asset_href (str, optional): asset's URI (default to input path).
        with_proj (bool): Add the `projection` extension and properties (default to False).
        with_raster (bool): Add the `raster` extension and properties (default to False).
        with_eo (bool): Add the `eo` extension and properties (default to False).
        raster_max_size (int): Limit array size from which to get the raster statistics. Defaults to 1024.
        geom_densify_pts (int): Number of points to add to each edge to account for nonlinear edges transformation (Note: GDAL uses 21).
        geom_precision (int): If >= 0, geometry coordinates will be rounded to this number of decimal.

    Returns:
        pystac.Item: valid STAC Item.

    """
    properties = properties or {}
    extensions = extensions or []
    asset_roles = asset_roles or []

    with ExitStack() as ctx:
        if isinstance(source, (DatasetReader, DatasetWriter, WarpedVRT)):
            dataset = source
        else:
            dataset = ctx.enter_context(rasterio.open(source))

        if dataset.gcps[0]:
            src_dst = ctx.enter_context(
                WarpedVRT(
                    dataset,
                    src_crs=dataset.gcps[1],
                    src_transform=transform.from_gcps(dataset.gcps[0]),
                )
            )
        else:
            src_dst = dataset

        dataset_geom = rioxarray_get_dataset_geom(
            src_dst,
            densify_pts=geom_densify_pts,
            precision=geom_precision,
        )

        media_type = (
            get_media_type(dataset) if asset_media_type == "auto" else asset_media_type
        )

        if "start_datetime" not in properties and "end_datetime" not in properties:
            # Try to get datetime from https://gdal.org/user/raster_data_model.html#imagery-domain-remote-sensing
            dst_date = src_dst.get_tag_item("ACQUISITIONDATETIME", "IMAGERY")
            dst_datetime = str_to_datetime(dst_date) if dst_date else None

            input_datetime = (
                input_datetime or dst_datetime or datetime.datetime.utcnow()
            )

        # add projection properties
        if with_proj:
            extensions.append(
                f"https://stac-extensions.github.io/projection/{PROJECTION_EXT_VERSION}/schema.json",
            )

            properties.update(
                {
                    f"proj:{name}": value
                    for name, value in rioxarray_get_projection_info(src_dst).items()
                }
            )

        # add raster properties
        raster_info = {}
        if with_raster:
            extensions.append(
                f"https://stac-extensions.github.io/raster/{RASTER_EXT_VERSION}/schema.json",
            )

            raster_info = {
                "raster:bands": rioxarray_get_raster_info(
                    dataset, max_size=raster_max_size
                )
            }

        eo_info: Dict[str, List] = {}
        if with_eo:
            extensions.append(
                f"https://stac-extensions.github.io/eo/{EO_EXT_VERSION}/schema.json",
            )

            eo_info = {"eo:bands": get_eobands_info(src_dst)}

            cloudcover = src_dst.get_tag_item("CLOUDCOVER", "IMAGERY")
            if cloudcover is not None:
                properties.update({"eo:cloud_cover": int(cloudcover)})

    # item
    item = pystac.Item(
        id=id or os.path.basename(dataset.name),
        geometry=dataset_geom["footprint"],
        bbox=dataset_geom["bbox"],
        collection=collection,
        stac_extensions=extensions,
        datetime=input_datetime,
        properties=properties,
    )

    # if we add a collection we MUST add a link
    if collection:
        item.add_link(
            pystac.Link(
                pystac.RelType.COLLECTION,
                collection_url or collection,
                media_type=pystac.MediaType.JSON,
            )
        )

    # item.assets
    if assets:
        for key, asset in assets.items():
            item.add_asset(key=key, asset=asset)

    else:
        item.add_asset(
            key=asset_name,
            asset=pystac.Asset(
                href=asset_href or dataset.name,
                media_type=media_type,
                extra_fields={**raster_info, **eo_info},
                roles=asset_roles,
            ),
        )

    return item
