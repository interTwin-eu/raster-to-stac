# STAC Metadata 
With this project, STAC metadata is extracted from satellitar imageries (input: NetCDF, Zarr, ...)
## TODO

Example of raster2stac lib test:

```python
import sys
sys.path.append("path/to/raster2stac")
from raster2stac import raster2stac as r2slib

r2s = r2slib.Raster2STAC(
    "/home/lmercurio/dev/raster-to-stac/data/test_michele/S2_L2A_sample.nc",
    output_folder="./results/",
    collection_id="test-collection-1",
    description="This is the description",
    output_file='test_collection.json',
    stac_version="1.0.0",
    verbose=True,
    s3_upload=True,
    version="1.0",
    providers=[
    {
        "url": "http://www.eurac.edu",
        "name": "Eurac EO WCS",
        "roles": [
            "host"
        ]
    }
    ],
    output_format="csv",
    title="This is a test collection",
    collection_url="https://url-to-coll.col/collection",
    license="test-license",
    write_json_items=True,
    keywords=['key1', 'key2', 'key3', 'key4'],
    sci_citation='N/A'
)

r2s.generate_stac()
```