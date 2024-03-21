from setuptools import setup

setup(
   name="raster2stac",
   version="0.0.1",
   description="Create valid STAC Collections, Items and Assets given already existing raster datasets",
   author="Michele Claus",
   author_email="michele.claus@eurac.edu",
   packages=["raster2stac"],  #same as name
   install_requires=[
       "numpy",
       "pandas",
       "xarray",
       "rioxarray",
       "pystac",
       "rio_stac",
       "boto3",
       "botocore",
       "openeo_processes_dask",
   ], #external packages as dependencies
)
