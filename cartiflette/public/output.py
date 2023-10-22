# -*- coding: utf-8 -*-
from datetime import date
import geopandas as gpd
import logging
import os
import pandas as pd
import s3fs
import shutil
import tempfile
import typing

import cartiflette
from cartiflette.download.scraper import MasterScraper
from cartiflette.s3.s3 import standardize_inputs
from cartiflette.utils import create_path_bucket

logger = logging.getLogger(__name__)

def download_file_single(
    year: typing.Union[str, int],
    borders: str,
    crs: typing.Union[str, int],
    filter_by: str,
    value: typing.Union[str, int, float],
    file_format: str,
    provider: str,
    source: str,
    simplication: typing.Union[str, int, float] = 0,
    dataset_family: str,
    territory: str,
    filename: str = "*",
    type_download: str = "https",
    fs: s3fs.S3FileSystem = cartiflette.FS,
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
    endpoint_url: str = cartiflette.ENDPOINT_URL,
    *args,
    **kwargs,
) -> typing.Union[gpd.GeoDataFrame, gpd.pd.DataFrame]:
    """


    Parameters
    ----------
    year : typing.Union[str, int]
        The year of the file. If not defined, will be set according to
        the current date's year.
    borders : str
        The administrative borders of the tiles within the vector file. Can be
        any administrative borders provided by IGN, e.g. "COMMUNE",
        "DEPARTEMENT" or "REGION". Default is "COMMUNE". The default is
    crs : typing.Union[str, int]
        The coordinate reference system of the file (if geographic data). Use
        None if targetting a plain tabular file.
    filter_by : str
        The administrative borders (supra to 'borders') that will be used to
        cut the vector file in pieces when writing to S3. For instance, if
        borders is "DEPARTEMENT", filter_by can be "REGION" or
        "FRANCE_ENTIERE".
    value : typing.Union[str, int, float]
        The value of the vector file (associated to the `filter_by` argument).
    file_format : str
        The format of the file, can be "geojson", "topojson", "gpkg" or
        "shp", "csv", ...
    provider : str
        Dataset's provider as described in the yaml config file. ("IGN" for
        instance)
    source : str
        Dataset's source as described in the yaml file.
        ("EXPRESS-COG-TERRITOIRE" of instance)
    dataset_family : str
        Dataset's family as described in the yaml file. ("ADMINEXPRESS" for
        instance).
    territory : str
        Dataset's territory. ("france_entiere" for instance).
    filename : str, optional
        Dataset's filename on storage system. Use "*" to catch every
        file in the assigned bucket storage path. Default is "*".
    type_download : str, optional
        The download's type to perform. Can be either "https" or "bucket".
        The default is "https".
    fs : s3fs.S3FileSystem, optional
        The s3 file system to use (in case of "bucket" download type). The
        default is cartiflette.FS.
    bucket : str, optional
        The name of the bucket where the file is stored. The default is
        cartiflette.BUCKET.
    path_within_bucket : str, optional
        The path within the bucket where the file will be stored. The default
        is cartiflette.PATH_WITHIN_BUCKET.
    endpoint_url: str, optional
        The http path to access the public path to the storage system. The
        default is cartiflette.ENDPOINT_URL.
    *args : TYPE
        Arguments passed to requests.Session (in case of "https" download type)
    **kwargs : TYPE
        Arguments passed to requests.Session (in case of "https" download type)

    Raises
    ------
    ValueError
        If type_download not among "https", "bucket".

    Returns
    -------
    gdf : typing.Union[gpd.GeoDataFrame, gpd.pd.DataFrame]
        The dataframe as a (Geo)Pandas object

    """

    if not year:
        year = str(date.today().year)

    corresp_filter_by_columns, format_read, driver = standardize_inputs(
        file_format
    )

    if type_download not in ("https", "bucket"):
        msg = (
            "type_download must be either 'https' or 'bucket' - "
            f"found '{type_download}' instead"
        )
        raise ValueError(msg)

    url = create_path_bucket(
        {
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "provider": provider,
            "dataset_family": dataset_family,
            "source": source,
            "year": year,
            "administrative_level": borders,
            "crs": crs,
            "filter_by": filter_by,
            "value": value,
            "territory": territory,
            "simplification": simplication,
            "file_format": format_read,
            "filename": filename,
        }
    )

    with tempfile.TemporaryDirectory() as tdir:
        if type_download == "bucket":
            try:
                if not fs.exists(url):
                    raise IOError(
                        f"File has not been found at path {url} on S3"
                    )
            except Exception:
                raise IOError(f"File has not been found at path {url} on S3")
            else:
                if format_read == "shp":
                    files = fs.ls(url)
                    for remote_file in files:
                        local_path = f"{tdir}/{remote_file.replace(url, '')}"
                        fs.download(remote_file, local_path)
                    local_path = f"{tdir}/raw.shp"

                else:
                    local_path = f"{tdir}/{os.path.basename(url)}"
                    fs.download(url, local_path)

        else:
            url = f"{endpoint_url}/{url}"
            with MasterScraper(*args, **kwargs) as s:
                # Note that python should cleanup all tmpfile by itself

                if format_read == "shp":
                    for ext in ["cpg", "dbf", "prj", "shp", "shx"]:
                        successes = []
                        for remote_file in files:
                            remote = os.path.splitext(url)[0] + f".{ext}"
                            (
                                success,
                                filetype,
                                tmp,
                            ) = s.download_to_tempfile_http(url=remote)
                            successes.append(success)
                            shutil.copy(tmp, f"{tdir}/raw.{ext}")
                        local_path = f"{tdir}/raw.shp"
                        success = any(successes)
                else:
                    (
                        success,
                        filetype,
                        local_path,
                    ) = s.download_to_tempfile_http(url=url)

            if not success:
                raise IOError("Download failed")

        if format_read == "parquet":
            try:
                gdf = gpd.read_parquet(local_path)
            except ValueError as e:
                if "Missing geo" in f"{e}":
                    gdf = pd.read_parquet(local_path)
                else:
                    raise e
        elif format_read == "csv":
            gdf = pd.read_csv(local_path)
        else:
            gdf = gpd.read_file(local_path, driver=driver)

    return gdf


# TODO : update des arguments
def download_file_multiple(
    bucket: str = cartiflette.BUCKET,
    path_within_bucket: str = cartiflette.PATH_WITHIN_BUCKET,
    provider: str = "IGN",
    source: str = "EXPRESS-COG-TERRITOIRE",
    file_format: str = "geojson",
    borders: str = "COMMUNE",
    filter_by: str = "region",
    year: typing.Union[str, int, float] = None,
    values: typing.Union[list, str, int, float] = "28",
    crs: typing.Union[list, str, int, float] = 2154,
    type_download: str = "https",
    fs: s3fs.S3FileSystem = cartiflette.FS,
    *args,
    **kwargs,
) -> gpd.GeoDataFrame:
    """
    This function performs multiple downloads of individual vector files (from
    a specified S3 bucket or an URL) and returns their concatenation as a
    GeoPandas object.

    Parameters
    ----------
    bucket : str, optional
        The name of the bucket where the file is stored. The default is
        cartiflette.BUCKET.
    path_within_bucket : str, optional
        The path within the bucket where the file will be stored. The default
        is cartiflette.PATH_WITHIN_BUCKET.
    provider : str, optional
        Dataset's provider as described in the yaml config file. The default is
        "IGN".
    source : str, optional
        Dataset's source as described in the yaml file. The default is
        "EXPRESS-COG-TERRITOIRE".
    file_format : str, optional
        The format of the vector file, can be "geojson", "topojson", "gpkg" or
        "shp". The default is "geojson".
    borders : str, optional
        The administrative borders of the tiles within the vector file. Can be
        any administrative borders provided by IGN, e.g. "COMMUNE",
        "DEPARTEMENT" or "REGION". Default is "COMMUNE". The default is
        "COMMUNE".
    filter_by : str, optional
        The administrative borders (supra to 'borders') that will be used to
        cut the vector file in pieces when writing to S3. For instance, if
        borders is "DEPARTEMENT", filter_by can be "REGION" or
        "FRANCE_ENTIERE". The default is "region".
    year : typing.Union[str, int, float], optional
        The year of the vector file. The default is the current date's year.
    values : typing.Union[list, str, int, float], optional
        The values of the vector files (associated to the `filter_by`
        argument). In case of multiple files, a concatenation will be
        performed. The default is "28".
    crs : typing.Union[str, int, float], optional
        The coordinate reference system of the vector file. The default is
        2154.
    type_download : str, optional
        The download's type to perform. Can be either "https" or "bucket".
        The default is "https".
    fs : s3fs.S3FileSystem, optional
        The s3 file system to use (in case of "bucket" download type). The
        default is cartiflette.FS.
    *args
        Arguments passed to requests.Session (in case of "https" download type)
    **kwargs
        Arguments passed to requests.Session (in case of "https" download type)

    Raises
    ------
    ValueError
        If type_download not among "https", "bucket".

    Returns
    -------
    gdf : gpd.GeoDataFrame
        The vector file as a GeoPandas object

    """

    if not year:
        year = str(date.today().year)

    if isinstance(values, (str, int, float)):
        values = [str(values)]

    if type_download not in ("https", "bucket"):
        msg = (
            "type_download must be either 'https' or 'bucket' - "
            f"found '{type_download}' instead"
        )
        raise ValueError(msg)

    vectors = [
        download_file_single(
            bucket=bucket,
            path_within_bucket=path_within_bucket,
            provider=provider,
            source=source,
            file_format=file_format,
            borders=borders,
            filter_by=filter_by,
            year=year,
            value=val,
            crs=crs,
            type_download=type_download,
            fs=fs,
            *args**kwargs,
        )
        for val in values
    ]

    vectors = gpd.pd.concat(vectors)

    return vectors
