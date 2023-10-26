# -*- coding: utf-8 -*-

from cartiflette.public.output import (
    download_file_multiple,
    download_file_single,
)

from cartiflette.public.client import (
    get_cog_year,
    get_vectorfile_ign,
    get_vectorfile_communes_arrondissement,
    get_living_area_commune,
    get_living_area,
)

__all__ = [
    "download_file_multiple",
    "download_file_single",
    "get_cog_year",
    "get_vectorfile_ign",
    "get_vectorfile_communes_arrondissement",
    "get_living_area_commune",
    "get_living_area",
]
