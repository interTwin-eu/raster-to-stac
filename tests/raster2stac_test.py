import os
import sys
import json
from pathlib import Path
import tempfile
import numpy as np
import pandas as pd
import xarray as xr

import pytest
from unittest.mock import patch, MagicMock

from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

sys.path.append("/home/rbalogun/raster-to-stac/")
from raster2stac import Raster2STAC
from stac_validator import stac_validator


@pytest.fixture
def r2s_sample_data_array() -> xr.DataArray:
    """xr.DataArray fixture for testing Raster2STAC

    Returns:
        xr.DataArray: sample dataarray
    """
    data = np.array(
        [
            [
                [2.1, 2.2, 0.8, 3.5, 1.9],
                [4.1, 0.5, 1.9, 4.6, 2.3],
                [2.9, 1.3, 3.7, 0.3, 5.7],
                [1.7, 4.4, 2.5, 2.2, 3.9],
                [3.2, 3.9, 5.6, 1.4, 4.6],
            ]
        ]
    )

    # Create the DataArray
    sample_ds = xr.DataArray(
        data=data,
        dims=["band", "y", "x"],
        coords={
            "x": [10.1, 10.5, 10.8, 11.2, 11.8],
            "y": [25.8, 26.4, 27.0, 27.6, 28.2],
            "band": np.array([1]),
        },
        attrs={
            "description": "sample dataarray fixture for testing Raster2STAC",
            "long_name": "raster2stac_dataarray",
            "_FillValue": -999,
            "scale_factor": 1.0,
            "add_offset": 0.0,
        },
    )

    # Add temporal dimension
    sample_ds = sample_ds.drop_vars("band").squeeze("band")
    sample_ds = sample_ds.expand_dims(t=pd.date_range("2022-01-01", periods=1), axis=0)

    # Add spatial attributes
    sample_ds = sample_ds.rio.write_crs("EPSG:4326")
    sample_ds = sample_ds.rio.set_spatial_dims(x_dim="x", y_dim="y")
    sample_ds = sample_ds.to_dataset(name="sample")
    sample_ds = sample_ds.to_dataarray(dim="band")

    return sample_ds


@pytest.fixture
def r2s_sample_dataset() -> xr.Dataset:
    """xr.Dataset fixture for testing Raster2STAC

    Returns:
        xr.Dataset: sample xr.Dataset
    """
    np.random.seed(42)
    data_one = np.random.rand(25, 25)
    data_two = np.random.rand(25, 25)
    lat = np.linspace(32, 45, 25)
    lon = np.linspace(52, 60, 25)

    sample_dataset = xr.Dataset(
        {
            "raster2stac_dataset_band1": (("lat", "lon"), data_one),
            "raster2stac_dataset_band2": (("lat", "lon"), data_two),
        },
        coords={
            "lat": lat,
            "lon": lon,
        },
        attrs={
            "description": "sample xarray Dataset fixture for testing Raster2STAC",
            "_FillValue": np.nan,
            "scale_factor": 0.1,
            "add_offset": 0.0,
        },
    )
    sample_dataset = sample_dataset.expand_dims(dim={"time": ["2024-06-06"]}, axis=0)
    # add spatial attributes
    ds = sample_dataset.rio.write_crs("EPSG:4326")
    ds = ds.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
    ds = ds.rename_dims({"lon": "x", "lat": "y"})
    return ds


@pytest.fixture
def r2s_nc(r2s_sample_data_array, tmp_path_factory):
    r2s_nc_path = tmp_path_factory.mktemp("NC") / "r2s_nc.nc"
    r2s_sample_data_array.to_netcdf(r2s_nc_path)
    return str(r2s_nc_path)


@pytest.fixture
def r2s_multi_band_cog_file(r2s_sample_dataset, tmp_path_factory):
    """create a temporary multi-band cloud-optimized geotiff and
    store in a temporary directory

    Yields:
        Iterator[tempfile.TemporaryDirectory]: temporary directory pointing to the COG file
    """

    config = dict(
        GDAL_NUM_THREADS="ALL_CPUS",
        GDAL_TIFF_INTERNAL_MASK=True,
        GDAL_TIFF_OVR_BLOCKSIZE="64",
    )

    r2s_tiff_path = tmp_path_factory.mktemp("RAST") / "raster2stac_multiband.tif"
    r2s_cog_path = tmp_path_factory.mktemp("COG") / "raster2stac_cog.tif"
    sample_data = r2s_sample_dataset.isel(time=0)
    sample_data.rio.to_raster(r2s_tiff_path)

    cog_translate(
        r2s_tiff_path, r2s_cog_path, cog_profiles.get("deflate"), config=config
    )
    return r2s_cog_path


@pytest.fixture
def r2s_kerchunk_file(r2s_sample_dataset, tmp_path_factory) -> list:
    """generates a list of netCDF files stored in a temporary directory to serve as input
    for testing the `generate_kerchunk_stac` method.

    Args:
        r2s_sample_dataset (pytest.fixture | xr.Dataset): xr.Dataset fixture for testing

    Returns:
        list: a list of temporary directories to netcdf files
    """
    nc_files = []
    for i in range(3):
        nc_file = tmp_path_factory.mktemp("kerchunk") / f"r2s_nc_file_{i}.nc"
        r2s_sample_dataset.to_netcdf(nc_file)
        nc_files.append([str(nc_file)])

    return nc_files


def test_raster2stac_init(r2s_sample_data_array):
    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = os.path.join(tmpdir, "R2S_TEST_COLLECTION")

    r2s = Raster2STAC(
        data=r2s_sample_data_array,
        collection_id="R2S_TEST_COLLECTION",
        collection_url="http://10.8.244.74:8082/collections/",  # TODO: create a mock up for this url,
        item_prefix="R2S_TEST",
        output_folder=out_path,  # TODO: use a temporary directory
        description="Test Collection generated for the Raster2STAC library intended for testing/validating the functionalities of the modules and functions",
        title="Raster2STAC Test Collection",
        ignore_warns=True,
        keywords=["test", "stac", "collection", "validation"],
        providers=[
            {
                "url": "http://www.eurac.edu",
                "name": "Eurac Research - Institute for Earth Observation",
                "roles": ["producer"],
            },
            {
                "url": "http://www.eurac.edu",
                "name": "Eurac Research - Institute for Earth Observation",
                "roles": ["host"],
            },
        ],
        stac_version="1.0.0",
        s3_upload=False,
        license="CC-BY-4.0",
        sci_citation="Balogun R.O. and Claus M., Raster2STAC: Generating STAC JSONs from Raster datasets. 2024, https://gitlab.inf.unibz.it/earth_observation_public/raster-to-stac",
    )

    assert r2s.data is r2s_sample_data_array
    assert r2s.collection_id == "R2S_TEST_COLLECTION"
    assert r2s.output_folder is out_path
    assert r2s.providers == [
        {
            "url": "http://www.eurac.edu",
            "name": "Eurac Research - Institute for Earth Observation",
            "roles": ["producer"],
        },
        {
            "url": "http://www.eurac.edu",
            "name": "Eurac Research - Institute for Earth Observation",
            "roles": ["host"],
        },
    ]
    assert r2s.s3_upload is False


@pytest.mark.parametrize(
    "test_data",
    [
        "r2s_sample_data_array",
        "r2s_sample_dataset",
        "r2s_nc",
        # "r2s_multi_band_cog_file", # FIX: read dataset from file with Raster2STAC
        # "r2s_kerchunk_file" # FIX: returns a couple of Pydantic v2 errors related to the spatial and temporal extent (see openeo_pg_parser_networkx)
    ],
)
def test_generate_stac(request, test_data):
    input_data = request.getfixturevalue(test_data)

    if test_data == "r2s_sample_data_array":
        assert isinstance(input_data, xr.DataArray)
    elif test_data == "r2s_sample_dataset":
        assert isinstance(input_data, xr.Dataset)
    elif test_data == "r2s_nc":
        assert isinstance(input_data, str)
        input_data = (
            xr.open_dataarray(input_data)
            if "dataarray" in input_data
            else xr.open_dataset(input_data)
        )
        if len(input_data.dims) > 3:
            input_data = input_data.isel(band=0)
        assert isinstance(input_data, (xr.DataArray, xr.Dataset))
    elif test_data == "r2s_kerchunk_file":
        assert isinstance(input_data, list)

    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = os.path.join(tmpdir, "R2S_TEST_COLLECTION")

        r2s = Raster2STAC(
            data=input_data,
            collection_id="R2S_TEST_COLLECTION",
            collection_url="http://10.8.244.74:8082/collections/",  # TODO: create a mock up for this URL
            item_prefix="R2S_TEST",
            output_folder=out_path,  # TODO: use a temporary directory
            description="Test Collection generated for the Raster2STAC library intended for testing/validating the functionalities of the modules and functions",
            title="Raster2STAC Test Collection",
            ignore_warns=True,
            keywords=["test", "stac", "collection", "validation"],
            providers=[
                {
                    "url": "http://www.eurac.edu",
                    "name": "Eurac Research - Institute for Earth Observation",
                    "roles": ["producer"],
                },
                {
                    "url": "http://www.eurac.edu",
                    "name": "Eurac Research - Institute for Earth Observation",
                    "roles": ["host"],
                },
            ],
            stac_version="1.0.0",
            s3_upload=False,
            license="CC-BY-4.0",
            sci_citation="Balogun R.O. and Claus M., Raster2STAC: Generating STAC JSONs from Raster datasets. 2024, https://gitlab.inf.unibz.it/earth_observation_public/raster-to-stac",
        )

        if (
            test_data == "r2s_kerchunk_file"
        ):  # FIX: Test if this logic works, then include kerchunk in parametrize
            r2s.generate_kerchunk_stac()
        else:
            r2s.generate_cog_stac()

        stac = stac_validator.StacValidate()
        output_path = Path(out_path) / "R2S_TEST_COLLECTION.json"
        assert output_path.exists()
        with open(output_path, "r") as f:
            stac_collection = json.load(f)
            assert stac_collection["id"] == "R2S_TEST_COLLECTION"
            stac.validate_dict(stac_collection)
            assert stac.message[0]["valid_stac"]


# TODO: mock EURAC-EO AWS S3 Bucket


@patch("boto3.client")
def test_upload_s3(mock_boto_client, r2s_sample_data_array):
    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = os.path.join(tmpdir, "S3_COLLECTION")

    """Test upload_s3 method."""
    mock_s3_client = MagicMock()
    mock_boto_client.return_value = mock_s3_client

    r2s = Raster2STAC(
        data=r2s_sample_data_array,
        collection_id="S3_COLLECTION",
        collection_url="http://10.8.244.74:8082/collections/",
        output_folder=out_path,
        description="Test  Collection for uploading to S3 bucket",
        title="S3 Collection Upload",
        ignore_warns=True,
        keywords=["s3", "bucket", "upload"],
        providers=[
            {
                "url": "http://www.eurac.edu",
                "name": "Eurac Research - Institute for Earth Observation",
                "roles": ["producer"],
            },
            {
                "url": "http://www.eurac.edu",
                "name": "Eurac Research - Institute for Earth Observation",
                "roles": ["host"],
            },
        ],
        stac_version="1.0.0",
        s3_upload=True,
        bucket_name="sample_bucket",
        bucket_file_prefix="sample_prefix",
    )

    r2s.upload_s3(str(out_path))

    mock_s3_client.upload_file.assert_called_with(
        str(out_path), "sample_bucket", "sample_prefix/S3_COLLECTION"
    )


if __name__ == "__main__":
    pytest.main()
