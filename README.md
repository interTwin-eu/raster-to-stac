# Raster-to-STAC  
`raster-to-stac` library allows to extract STAC metadata from raster satellite imageries -(as xarrays), organizing it in catalogs and items

## Requirements

## Installation

## Usage
Example of raster2stac lib usage:

```python
import sys
sys.path.append("path/to/raster2stac")
from raster2stac import raster2stac as r2slib

r2s = r2slib.Raster2STAC(
    "/path/to/nc/S2_L2A_sample.nc", # data
    "test-collection-1", # collection_id
    "https://url-to-coll.col/collection", # collection_url
    output_folder="./results/",
    output_file='test_collection.json',
    description="This is the description",
    title="This is a test collection",
    ignore_warns=False,
    keywords=['key1', 'key2', 'key3', 'key4'],
    providers=[
    {
        "url": "http://www.eurac.edu",
        "name": "Eurac EO WCS",
        "roles": [
            "host"
        ]
    }
    ],
    stac_version="1.0.0",
    verbose=True,
    s3_upload=True,
    version="1.0",
    output_format="csv",
    collection_url="",
    license="test-license",
    write_json_items=True,
    sci_citation='N/A'
)

r2s.generate_stac()
```


## License

This project is distributed with MIT license - see 'LICENSE' for details.