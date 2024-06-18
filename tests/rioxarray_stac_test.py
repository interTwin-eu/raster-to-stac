import numpy as np
import xarray as xr
import pytest
import rasterio

from raster2stac.rioxarray_stac import (
    bbox_to_geom,
    rioxarray_get_dataset_geom,
    rioxarray_get_projection_info,
    get_eobands_info,
    _rioxarray_get_stats,
    rioxarray_get_raster_info,
    get_media_type,
)


@pytest.fixture
def sample_data_array():
    data = np.random.rand(1, 1, 5, 5)
    coords = {
        "band": [1],
        "t": [np.datetime64("2022-01-01")],
        "y": np.linspace(25, 30, 5),
        "x": np.linspace(10, 15, 5),
    }
    attrs = {
        "crs": "EPSG:4326",
        "scale_factor": 1.0,
        "add_offset": 0.0,
    }
    return xr.DataArray(data, coords=coords, dims=["band", "t", "y", "x"], attrs=attrs)


def test_bbox_to_geom():
    bbox = (-180.0, -90.0, 180.0, 90.0)
    geom = bbox_to_geom(bbox)
    expected_geom = {
        "type": "Polygon",
        "coordinates": [
            [
                [-180.0, -90.0],
                [180.0, -90.0],
                [180.0, 90.0],
                [-180.0, 90.0],
                [-180.0, -90.0],
            ]
        ],
    }
    assert geom == expected_geom


def test_rioxarray_get_dataset_geom(sample_data_array):
    geom_info = rioxarray_get_dataset_geom(sample_data_array)
    assert "bbox" in geom_info
    assert "footprint" in geom_info


def test_rioxarray_get_projection_info(sample_data_array):
    projection_info = rioxarray_get_projection_info(sample_data_array)
    assert "epsg" in projection_info
    assert projection_info["epsg"] == 4326


def test_get_eobands_info():
    with rasterio.open(
        "test.tif", "w", driver="GTiff", height=10, width=10, count=3, dtype="uint8"
    ) as src_dst:
        eo_bands = get_eobands_info(src_dst)
        assert len(eo_bands) == 3


def test_rioxarray_get_stats(sample_data_array):
    stats = _rioxarray_get_stats(sample_data_array)
    assert "statistics" in stats
    assert "histogram" in stats


def test_rioxarray_get_raster_info(sample_data_array):
    raster_info = rioxarray_get_raster_info(sample_data_array)
    assert isinstance(raster_info, list)
    assert "data_type" in raster_info[0]


def test_get_media_type():
    with rasterio.open(
        "sample.tif", "w", driver="GTiff", height=10, width=10, count=3, dtype="uint8"
    ) as src_dst:
        media_type = get_media_type(src_dst)
        assert media_type == "image/tiff"


if __name__ == "__main__":
    pytest.main()
