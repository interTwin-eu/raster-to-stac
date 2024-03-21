

__title__ = "raster2stac"


from raster2stac._version import __version__
from raster2stac.raster2stac import Raster2STAC


def client_version() -> str:
    try:
        import importlib.metadata
        return importlib.metadata.version("raster2stac")
    except Exception:
        return __version__
