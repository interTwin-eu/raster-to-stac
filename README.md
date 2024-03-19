# Raster-to-STAC  
This component allows the creation of STAC Collection with Items and Assets starting from different kinds of raster datasets. It also allows the user to automatically upload the resulting files to an Amazon S3 Bucket, to make them publicly accessible and reachable worldwide. The goal is to make a dataset easily accessible, interoperable, and shareable.

Depending on the requirements, two approaches can be taken:

1. Via COGs
The first approach, will read the input dataset and write several Cloud Optimized Geotiffs to the local disk. One COG per time stamp and per variable/band in the netCDF will be generated. This might increase the overall memory required to store the data, but allows a high level of interoperability with third party libraries for reading and visualizing the data.

2. Via Kerchunk
The second approach tries to keep the original data as is, without the necessity to duplicate it in COGs. The sample use case that we will cover consist in netCDF files and for each of the a JSON Kerchunk file will be created. The Kerchunk files will be then read by raster2stac and a SATC Collection generated.

## Installation

```
pip install .
```

## Usage Examples

### Case 1: convert netCDF to COGs and create a STAC Collection with Items and Assets

1. Get a sample netCDF file:
```
wget https://github.com/Open-EO/openeo-localprocessing-data/raw/main/sample_netcdf/S2_L2A_sample.nc
```
2. Call raster2stac:

```python
from raster2stac import Raster2STAC

rs2stac = Raster2STAC(
    data = "S2_L2A_sample.nc", # The netCDF which will be converted into COGs
    collection_id = "SENTINEL2_L2A_SAMPLE", # The Collection id we want to set
    collection_url = "https://stac.eurac.edu/collections/", # The URL where the collection will be exposed
    output_folder="SENTINEL2_L2A_SAMPLE_STAC"
).generate_cog_stac()
```

3. Reload the data via a STAC Item we just generated:
```python
import json
import pystac_client
import odc.stac

item_path = "./SENTINEL2_L2A_SAMPLE_STAC/items/20220630000000.json"
stac_api = pystac_client.stac_api_io.StacApiIO()
stac_dict = json.loads(stac_api.read_text(item_path))
item = stac_api.stac_object_from_dict(stac_dict)

ds_stac = odc.stac.load([item])
print(ds_stac)

> <xarray.Dataset> Size: 13MB
> Dimensions:      (y: 705, x: 935, time: 1)
> Coordinates:
>   * y            (y) float64 6kB 5.155e+06 5.155e+06 ... 5.148e+06 5.148e+06
>   * x            (x) float64 7kB 6.75e+05 6.75e+05 ... 6.843e+05 6.843e+05
>     spatial_ref  int32 4B 32632
>   * time         (time) datetime64[ns] 8B 2022-06-30
> Data variables:
>     B04          (time, y, x) float32 3MB 278.0 302.0 274.0 ... 306.0 236.0
>     B03          (time, y, x) float32 3MB 506.0 520.0 456.0 ... 378.0 367.0
>     B02          (time, y, x) float32 3MB 237.0 240.0 249.0 ... 246.0 212.0
>     B08          (time, y, x) float32 3MB 3.128e+03 2.958e+03 ... 1.854e+03
>     SCL          (time, y, x) float32 3MB 4.0 4.0 4.0 4.0 ... 4.0 4.0 4.0 4.0

```

### Case 2: create Kerchunk files from a list of netCDFs and create a STAC Collection with Items and Assets

1. Get sample netCDF files:
```
wget https://eurac-eo.s3-eu-west-1.amazonaws.com/INTERTWIN/CERRA/NETCDF/t2m_2001_crs.nc
wget https://eurac-eo.s3-eu-west-1.amazonaws.com/INTERTWIN/CERRA/NETCDF/t2m_2002_crs.nc
wget https://eurac-eo.s3-eu-west-1.amazonaws.com/INTERTWIN/CERRA/NETCDF/sp_2001_crs.nc
wget https://eurac-eo.s3-eu-west-1.amazonaws.com/INTERTWIN/CERRA/NETCDF/sp_2002_crs.nc
```

```python
# List of lists of netcdfs files we want to process
# In each sublist, one netCDF per variable, covering the same temporal range.
# Therefore, there will be N sublists for N temporal ranges.

netcdf_list = [["t2m_2001_crs.nc","sp_2001_crs.nc"],
               ["t2m_2002_crs.nc","sp_2002_crs.nc"]]
```

2. Call raster2stac:

```python
from raster2stac import Raster2STAC

r2s = Raster2STAC(
    data = netcdf_list,
    collection_id = "CERRA", # collection_id
    collection_url = "https://stac.eurac.edu/collection", # collection_ur, the STAC API where we foresee to share this Collection
    output_folder="./cerra/kerchunk/",
    description="The Copernicus European Regional ReAnalysis (CERRA) datasets provide spatially and \
        temporally consistent historical reconstructions of meteorological variables in the atmosphere \
        and at the surface. ",
    title="CERRA sub-daily regional reanalysis data for the European Alps on single levels",
    ignore_warns=False,
    keywords=['intertwin', 'cerra', 'climate'],
    links= [{
        "rel": "license",
        "href": "https://cds.climate.copernicus.eu/api/v2/terms/static/licence-to-use-copernicus-products.pdf",
        "title": "License to use Copernicus Products"
    }],
    providers=[
        {
            "url": "https://cds.climate.copernicus.eu/cdsapp#!/dataset/10.24381/cds.622a565a",
            "name": "Copernicus",
            "roles": [
                "producer"
            ]
        },
        {
            "url": "https://cds.climate.copernicus.eu/cdsapp#!/dataset/10.24381/cds.622a565a",
            "name": "Copernicus",
            "roles": [
                "licensor"
            ]
        },
        {
            "url": "http://www.eurac.edu",
            "name": "Eurac EO",
            "roles": [
                "host"
            ]
        }
    ],
    stac_version="1.0.0",
    s3_upload=False,
    version=None,
    license="proprietary",
    sci_doi='https://doi.org/10.24381/cds.622a565a',
    sci_citation= "Schimanke S., Ridal M., Le Moigne P., Berggren L., Undén P., Randriamampianina R., Andrea U., \
        Bazile E., Bertelsen A., Brousseau P., Dahlgren P., Edvinsson L., El Said A., Glinton M., Hopsch S., \
        Isaksson L., Mladek R., Olsson E., Verrelle A., Wang Z.Q., (2021): CERRA sub-daily regional reanalysis \
        data for Europe on single levels from 1984 to present. Copernicus Climate Change Service (C3S) Climate \
        Data Store (CDS), DOI: 10.24381/cds.622a565a (Accessed on 15-02-2024)"
)

r2s.generate_kerchunk_stac()
```

3. Reload the data via the STAC Items we just generated:

```python
import pystac_client
import odc.stac
import json
import xarray as xr

url_1 = "./cerra/kerchunk/items/20020101000000_20021231000000.json"
url_2 = "./cerra/kerchunk/items/20010101000000_20011231000000.json"
stac_api = pystac_client.stac_api_io.StacApiIO()
stac_dict_1 = json.loads(stac_api.read_text(url_1))
item_1 = stac_api.stac_object_from_dict(stac_dict_1)
stac_dict_2 = json.loads(stac_api.read_text(url_2))
item_2 = stac_api.stac_object_from_dict(stac_dict_2)
items = [item_1,item_2]

datasets_list = []
for item in items:
    for asset in item.assets:
        data = xr.open_dataset(
            "reference://",
            engine="zarr",
            decode_coords="all",
            backend_kwargs={
                "storage_options": {
                    "fo":item.assets[asset].href,
                },
                "consolidated": False
            },chunks={}
        ).to_dataarray(dim="bands")
        datasets_list.append(data)
        # Need to create one Item per time/netCDF
data = xr.combine_by_coords(datasets_list,combine_attrs="drop_conflicts")
print(data)

> <xarray.DataArray (bands: 2, time: 730, latitude: 98, longitude: 163)> Size: 93MB
> dask.array<concatenate, shape=(2, 730, 98, 163), dtype=float32, chunksize=(1, 365, 98, 163), > > > > chunktype=numpy.ndarray>
> Coordinates:
>   * latitude     (latitude) float64 784B 43.55 43.62 43.69 ... 49.93 50.0 50.06
>   * longitude    (longitude) float64 1kB 5.084 5.151 5.218 ... 15.82 15.89 15.96
>     spatial_ref  float64 8B 0.0
>   * time         (time) datetime64[ns] 6kB 2001-01-01 2001-01-02 ... 2002-12-31
>   * bands        (bands) object 16B 'sp' 't2m'
> Attributes:
>     NCO:      netCDF Operators version 5.1.9 (Homepage = http://nco.sf.net, C...

```
## License

This project is distributed with MIT license - see 'LICENSE' for details.
