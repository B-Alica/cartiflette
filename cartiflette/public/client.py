# -*- coding: utf-8 -*-
"""
Created on Fri Oct 20 16:35:46 2023

@author: thomas.grandjean
"""

from datetime import date
import geopandas as gpd
import pandas as pd
import typing

import cartiflette
from cartiflette.public.output import download_file_single


class CogDict(typing.TypedDict):
    "Used only for typing hints"
    COMMUNE: pd.DataFrame
    CANTON: pd.DataFrame
    ARRONDISSEMENT: pd.DataFrame
    DEPARTEMENT: pd.DataFrame
    REGION: pd.DataFrame
    COLLECTIVITE: pd.DataFrame
    PAYS: pd.DataFrame


def get_cog_year(
    year: int = None,
    bucket=cartiflette.BUCKET,
    path_within_bucket=cartiflette.PATH_WITHIN_BUCKET,
    type_download: str = "https",
    fs=cartiflette.FS,
    **kwargs_requests,
) -> CogDict:
    """
    Retrieve unified COG data from the file system storage and returns a dict
    of all data.

    Parameters
    ----------
    year : int, optional
        Desired vintage. If None (default), will use the current date's year.
    bucket : str, optional
        Bucket to use. The default is cartiflette.BUCKET.
    path_within_bucket : str, optional
        path within bucket. The default is cartiflette.PATH_WITHIN_BUCKET.
    type_download : str, optional
        The download's type to perform. Can be either "https" or "bucket".
        The default is "https".
    fs : s3fs.S3FileSystem, optional
        The s3 file system to use (in case of "bucket" download type). The
        default is cartiflette.FS.

    *args : TYPE
        DESCRIPTION.
    **kwargs : TYPE
        DESCRIPTION.

    Returns
    -------
    CogDict
        Dictionnary of dataframes. Each key represent a "layer" of the COG's
        yearly dataset. It might change from year to year, according to what's
        really present in the dataset.

        If no data is present for the desired vintage, empty dataframes will be
        returned in the dictionnary.

        Ex. :
        {
            'COMMUNE':
                       TYPECOM    COM   REG  ...                  LIBELLE    CAN COMPARENT
                 0         COM  01001  84.0  ...  L'Abergement-Clémenciat   0108       NaN
                 1         COM  01002  84.0  ...    L'Abergement-de-Varey   0101       NaN
                 2         COM  01004  84.0  ...        Ambérieu-en-Bugey   0101       NaN
                 3         COM  01005  84.0  ...      Ambérieux-en-Dombes   0122       NaN
                 4         COM  01006  84.0  ...                  Ambléon   0104       NaN
                 ...
                 [37601 rows x 12 columns],

            'CANTON':
                      id_canton id_departement  ...            libelle  actual
                 0         0101             01  ...  Ambérieu-en-Bugey       C
                 1         0102             01  ...           Attignat       C
                 2         0103             01  ...         Valserhône       C
                 3         0104             01  ...             Belley       C
                 4         0105             01  ...  Bourg-en-Bresse-1       C
                 ...
                 [2290 rows x 10 columns],

            'ARRONDISSEMENT':
                       ARR  DEP  ...                   NCCENR                  LIBELLE
                 0     011   01  ...                   Belley                   Belley
                 1     012   01  ...          Bourg-en-Bresse          Bourg-en-Bresse
                 2     013   01  ...                      Gex                      Gex
                 3     014   01  ...                   Nantua                   Nantua
                 4     021   02  ...          Château-Thierry          Château-Thierry
                 ...
                 [332 rows x 8 columns],

            'DEPARTEMENT':
                      DEP  REG  ...                   NCCENR                  LIBELLE
                 0     01   84  ...                      Ain                      Ain
                 1     02   32  ...                    Aisne                    Aisne
                 2     03   84  ...                   Allier                   Allier
                 3     04   93  ...  Alpes-de-Haute-Provence  Alpes-de-Haute-Provence
                 4     05   93  ...             Hautes-Alpes             Hautes-Alpes
                 ...
                 [101 rows x 7 columns],

            'REGION':
                     REG CHEFLIEU  ...                      NCCENR                  LIBELLE
                 0     1    97105  ...                  Guadeloupe               Guadeloupe
                 1     2    97209  ...                  Martinique               Martinique
                 2     3    97302  ...                      Guyane                   Guyane
                 3     4    97411  ...                  La Réunion               La Réunion
                 4     6    97608  ...                     Mayotte                  Mayotte
                 5    11    75056  ...               Île-de-France            Île-de-France
                 ...
                 [18 rows x 6 columns],

            'COLLECTIVITE':
                     CTCD  ...                                            LIBELLE
                 0    01D  ...                     Conseil départemental de L'Ain
                 1    02D  ...                   Conseil départemental de L'Aisne
                 2    03D  ...                  Conseil départemental de L'Allier
                 3    04D  ...  Conseil départemental des Alpes-de-Haute-Provence
                 4    05D  ...             Conseil départemental des Hautes-Alpes
                 ...
                 [100 rows x 6 columns],

            'PAYS':
                        COG  ACTUAL  CAPAY  CRPAY  ...  ANCNOM CODEISO2 CODEISO3 CODENUM3
                 0    99101       1    NaN    NaN  ...     NaN       DK      DNK    208.0
                 1    99101       3  99102    NaN  ...     NaN       FO      FRO    234.0
                 2    99102       1    NaN    NaN  ...     NaN       IS      ISL    352.0
                 3    99103       1    NaN    NaN  ...     NaN       NO      NOR    578.0
                 4    99103       3    NaN    NaN  ...     NaN       BV      BVT     74.0
                 ...
                 [282 rows x 11 columns]
         }

    """

    if not year:
        year = date.today().year
    levels = [
        "COMMUNE",
        "CANTON",
        "ARRONDISSEMENT",
        "DEPARTEMENT",
        "REGION",
        "COLLECTIVITE",
        "PAYS",
    ]

    dict_cog = {}
    for level in levels:
        kwargs_cartiflette = {
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "year": year,
            "borders": level,
            "crs": None,
            "filter_by": None,
            "value": None,
            "file_format": "parquet",
            "provider": "cartiflette",
            "dataset_family": "COG",
            "source": level,
            "territory": "france_entiere",
            "filename": f"{level}.parquet",
            "file_format": "parquet",
            "fs": fs,
            "type_download": type_download,
        }
        dict_cog[level] = download_file_single(
            **kwargs_cartiflette, **kwargs_requests
        )
    return dict_cog


def get_vectorfile_ign(
    dataset_family: str = "ADMINEXPRESS",
    source: str = "EXPRESS-COG-TERRITOIRE",
    year: str = None,
    territory: str = "*",
    borders: str = "COMMUNE",
    crs: str = "*",
    filter_by: str = "origin",
    value: str = "raw",
    simplification: int = 0,
    bucket=cartiflette.BUCKET,
    path_within_bucket=cartiflette.PATH_WITHIN_BUCKET,
    type_download: str = "https",
    fs=cartiflette.FS,
    **kwargs_requests,
) -> gpd.GeoDataFrame:
    # TODO : docstring
    """
    gdf : gpd.GeoDataFrame
        Raw dataset as extracted from IGN. The projection/encoding of the files
        will have been standardised to 4326.
        Ex. :
                                 ID           NOM         NOM_M INSEE_COM  \
        0  COMMUNE_0000000009754033    Connangles    CONNANGLES     43076
        1  COMMUNE_0000000009760784       Vidouze       VIDOUZE     65462
        2  COMMUNE_0000000009742077     Fouesnant     FOUESNANT     29058
        3  COMMUNE_0000000009735245  Plougrescant  PLOUGRESCANT     22218
        4  COMMUNE_0000000009752504     Montcarra     MONTCARRA     38250

                   STATUT  POPULATION INSEE_CAN INSEE_ARR INSEE_DEP INSEE_REG  \
        0  Commune simple         137        11         1        43        84
        1  Commune simple         243        13         3        65        76
        2  Commune simple        9864        11         4        29        53
        3  Commune simple        1166        27         3        22        53
        4  Commune simple         569        24         2        38        84

          SIREN_EPCI                                           geometry  \
        0  200073419  POLYGON ((748166.100 6463826.600, 748132.400 6...
        1  200072106  POLYGON ((455022.600 6263681.900, 455008.000 6...
        2  242900660  MULTIPOLYGON (((177277.800 6756845.800, 177275...
        3  200065928  MULTIPOLYGON (((245287.300 6878865.100, 245288...
        4  200068542  POLYGON ((889525.800 6504614.500, 889525.600 6...

                               source
        0  IGN:EXPRESS-COG-TERRITOIRE
        1  IGN:EXPRESS-COG-TERRITOIRE
        2  IGN:EXPRESS-COG-TERRITOIRE
        3  IGN:EXPRESS-COG-TERRITOIRE
        4  IGN:EXPRESS-COG-TERRITOIRE
    """
    kwargs = {
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "year": year,
        "borders": borders,
        "crs": crs,
        "filter_by": filter_by,
        "value": value,
        "provider": "cartiflette",
        "dataset_family": dataset_family,
        "source": source,
        "territory": territory,
        "filename": f"{borders}.gpkg",
        "file_format": "GPKG",
        "fs": fs,
        "type_download": type_download,
        "simplification": 0,
    }
    gdf = download_file_single(**kwargs, **kwargs_requests)
    return gdf


def get_vectorfile_communes_arrondissement(
    year: str = None,
    bucket=cartiflette.BUCKET,
    path_within_bucket=cartiflette.PATH_WITHIN_BUCKET,
    type_download: str = "https",
    fs=cartiflette.FS,
    **kwargs_requests,
) -> gpd.GeoDataFrame:
    """
    #TODO

    Parameters
    ----------
    year : str, optional
        DESCRIPTION. The default is None.
    bucket : TYPE, optional
        DESCRIPTION. The default is cartiflette.BUCKET.
    path_within_bucket : TYPE, optional
        DESCRIPTION. The default is cartiflette.PATH_WITHIN_BUCKET.
    type_download : str, optional
        DESCRIPTION. The default is "https".
    fs : TYPE, optional
        DESCRIPTION. The default is cartiflette.FS.
    *args : TYPE
        DESCRIPTION.
    **kwargs : TYPE
        DESCRIPTION.

    Returns
    -------
    gdf : gpd.GeoDataFrame
        Ex:
                                 ID           NOM         NOM_M INSEE_COM  \
        0  COMMUNE_0000000009754033    Connangles    CONNANGLES     43076
        1  COMMUNE_0000000009760784       Vidouze       VIDOUZE     65462
        2  COMMUNE_0000000009742077     Fouesnant     FOUESNANT     29058
        3  COMMUNE_0000000009735245  Plougrescant  PLOUGRESCANT     22218
        4  COMMUNE_0000000009752504     Montcarra     MONTCARRA     38250

                   STATUT  POPULATION INSEE_CAN INSEE_ARR INSEE_DEP INSEE_REG  \
        0  Commune simple         137        11         1        43        84
        1  Commune simple         243        13         3        65        76
        2  Commune simple        9864        11         4        29        53
        3  Commune simple        1166        27         3        22        53
        4  Commune simple         569        24         2        38        84

          SIREN_EPCI                                           geometry  \
        0  200073419  POLYGON ((748166.100 6463826.600, 748132.400 6...
        1  200072106  POLYGON ((455022.600 6263681.900, 455008.000 6...
        2  242900660  MULTIPOLYGON (((177277.800 6756845.800, 177275...
        3  200065928  MULTIPOLYGON (((245287.300 6878865.100, 245288...
        4  200068542  POLYGON ((889525.800 6504614.500, 889525.600 6...

                               source INSEE_COG
        0  IGN:EXPRESS-COG-TERRITOIRE     43076
        1  IGN:EXPRESS-COG-TERRITOIRE     65462
        2  IGN:EXPRESS-COG-TERRITOIRE     29058
        3  IGN:EXPRESS-COG-TERRITOIRE     22218
        4  IGN:EXPRESS-COG-TERRITOIRE     38250


    """
    source = "EXPRESS-COG-TERRITOIRE"
    territory = "france_entiere"
    dataset_family = "ADMINEXPRESS"
    kwargs = {
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "year": year,
        "borders": "COMMUNE",
        "crs": 4326,
        "filter_by": None,
        "value": None,
        "file_format": "GPKG",
        "provider": "cartiflette",
        "dataset_family": dataset_family,
        "source": source,
        "territory": territory,
        "filename": "COMMUNE_ARRONDISSEMENTS_MUNICIPAUX.gpkg",
        "fs": fs,
        "type_download": type_download,
        "simplification": 0,
    }
    gdf = download_file_single(**kwargs, **kwargs_requests)
    return gdf


def get_living_area_commune(
    year=None,
    year_bv="2022",
    bucket=cartiflette.BUCKET,
    path_within_bucket=cartiflette.PATH_WITHIN_BUCKET,
    type_download: str = "https",
    fs=cartiflette.FS,
    **kwargs_requests,
):
    if not year:
        year = str(date.today().year)
    kwargs = {
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "year": year,
        "borders": "COMMUNE",
        "crs": 4326,
        "filter_by": None,
        "value": None,
        "file_format": "GPKG",
        "provider": "cartiflette",
        "dataset_family": f"bassins-vie-{year_bv}",
        "source": "BV",
        "territory": "france_entiere",
        "filename": "bassins_vie.gpkg",
        "fs": fs,
        "simplification": 0,
    }
    gdf = download_file_single(**kwargs, **kwargs_requests)
    return gdf


def get_living_area(
    year,
    year_bv,
    bucket=cartiflette.BUCKET,
    path_within_bucket=cartiflette.PATH_WITHIN_BUCKET,
    type_download: str = "https",
    fs=cartiflette.FS,
    **kwargs_requests,
):
    # TODO
    kwargs = {
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "year": year,
        "borders": "BASSIN-VIE",
        "crs": 4326,
        "filter_by": "origin",
        "value": "preprocessed",
        "file_format": "GPKG",
        "provider": "cartiflette",
        "dataset_family": f"bassins-vie-{year_bv}",
        "source": "BV",
        "territory": "france_entiere",
        "filename": "bassins_vie.gpkg",
        "fs": fs,
        "simplification": 0,
    }
    gdf = download_file_single(**kwargs, **kwargs_requests)
    return gdf


if __name__ == "__main__":
    # get_bv_commune(2022)
    # ret = get_cog_year(2022)
    ret = get_vectorfile_ign(year=2022)
