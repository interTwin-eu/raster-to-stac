

__title__ = "raster2stac"


from raster2stac._version import __version__


def client_version() -> str:
    try:
        import importlib.metadata
        return importlib.metadata.version("raster2stac")
    except Exception:
        return __version__