# Raster-to-STAC  
This package allows the creation of STAC Collection with Items and Assets starting from different kind of raster datasets.
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
    data = "S2_L2A_sample.nc",
    collection_id = "S2_L2A_SAMPLE",
    collection_url = "",
    output_folder="S2_L2A_SAMPLE_STAC"
).generate_stac()
```

3. Reload the data via a STAC Item we just generated:
```python
import json
import pystac
import pystac_client
import odc.stac

item_path = "./S2_L2A_SAMPLE_STAC/items-json/S2_L2A_SAMPLE-20220630000000.json"
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


## License

This project is distributed with MIT license - see 'LICENSE' for details.