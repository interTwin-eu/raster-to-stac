.. raster2stac documentation master file, created by
   sphinx-quickstart on Thu Apr 11 14:58:08 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to raster2stac's documentation!
=======================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:


raster2stac
===========
Raster2STAC is a python library that enables the creation of STAC Collections with its corresponding sub-items and assets starting from different kinds of raster datasets (ideally cloud-optimized data formats). 
It also allows the user to automatically upload the resulting files to an Amazon S3 Bucket, to make them publicly accessible and reachable worldwide. This way geospatial datasets can be easily accessible, 
interoperable and shareable, adhering to the core tenets of the FAIR principle.  


Main features
-------------
- Generates STAC Collection from CoGs, NetCDF and Kerchunk data formats, 
- Uploads to data to cloud storage (e.g. Amazon S3),
- Loads well with downstream packages like odc.stac
- Released under the `MIT License`


User guide
-----------

.. toctree::
   :maxdepth: 2

   guide/installation.rst
   guide/quickstart.rst
   guide/contributing.rst


License
-------
The Raster2STAC package is released under the MIT License

.. _MIT License: https://gitlab.inf.unibz.it/earth_observation_public/raster-to-stac/-/blob/rob_dev/LICENSE


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
