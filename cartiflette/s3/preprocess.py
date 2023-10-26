# -*- coding: utf-8 -*-

from datetime import date
from functools import partial
import geopandas as gpd
import io
import logging
import numpy as np
import os
import pandas as pd
from pebble import ThreadPool
import s3fs
import tempfile


from cartiflette import BUCKET, PATH_WITHIN_BUCKET, FS, THREADS_DOWNLOAD
from cartiflette.utils import magic_csv_reader, create_path_bucket
from cartiflette.public import download_file_single

logger = logging.getLogger(__name__)

# TODO : docstrings

logger.error("preprocessing des bassins de vie pose pb, manque Paris")


def store_cog_year(
    year: int = None,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
) -> None:
    """
    Retrieve all COG files on S3, concat all territories and store it into the
    storage system. To retrieve data, use cartiflette.public.get_cog_year
    instead.

    Parameters
    ----------
    year : int, optional
        Desired vintage. If None (default), will use the current date's year.
    bucket : str, optional
        Bucket to use. The default is BUCKET.
    path_within_bucket : str, optional
        path within bucket. The default is PATH_WITHIN_BUCKET.
    fs : s3fs.S3FileSystem, optional
        S3 file system to use. The default is FS.

    Returns
    -------
    None
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

    provider = "Insee"
    dataset_family = "COG"

    dict_cog = {}
    for source in levels:
        try:
            pattern = (
                f"{bucket}/{path_within_bucket}/"
                f"{provider=}/{dataset_family=}/{source=}/{year=}/**/*.*"
            ).replace("'", "")
            files = fs.glob(pattern)  # , refresh=True)
            # see issue : https://github.com/fsspec/s3fs/issues/504
            data = []
            for file in files:
                with fs.open(file, "rb") as f:
                    dummy = io.BytesIO(f.read())
                df = magic_csv_reader(dummy)
                data.append(df)
            if data:
                dict_cog[source] = pd.concat(data)
                dict_cog[source]["source"] = f"{provider}:{source}"
            else:
                dict_cog[source] = pd.DataFrame()

            for ext, method, kwargs in [
                ("parquet", "to_parquet", {}),
                # ("csv", "to_csv", {"encoding": "utf8"}),
            ]:
                config_dict = {
                    "bucket": bucket,
                    "path_within_bucket": path_within_bucket,
                    "year": year,
                    "borders": source,
                    "crs": None,
                    "filter_by": "origin",
                    "value": "preprocessed",
                    "file_format": ext,
                    "provider": "cartiflette",
                    "dataset_family": "COG",
                    "source": source,
                    "simplification": 0,
                    "territory": "france_entiere",
                    "filename": f"{source}.{ext}",
                }
                path = create_path_bucket(config=config_dict)
                with fs.open(path, "wb") as f:
                    getattr(dict_cog[source], method)(f, **kwargs)
        except Exception as e:
            logger.error(e)


def store_cog_ign(
    borders: str = "COMMUNE",
    year: str = None,
    territory: str = "metropole",
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
) -> None:
    """
    Retrieve IGN shapefiles from MinIO and store it after preprocessing; if
    multiple files are gathered, those will be concatenated. In any case, the
    projection will be uniformized to 4326.

    Note that 'territory' and 'borders' can use a "*" wildcard to concatenate
    all available datasets.

    Parameters
    ----------
    year : int, optional
        Desired vintage. Will use the current year if set to None (which is
        default).
    territory : str, optional
        Territory as described in the yaml file. The default is "metropole".
    borders : str, optional
        Desired "mesh" (ie available layers in the raw dataset : commune,
        arrondissement, etc.). The default is "COMMUNE".
    bucket : str, optional
        Bucket to use. The default is BUCKET.
    path_within_bucket : str, optional
        path within bucket. The default is PATH_WITHIN_BUCKET.
    fs : s3fs.S3FileSystem, optional
        S3 file system to use. The default is FS.


    Raises
    ------
    ValueError
        - If a wildcard is used on an unallowed argument
        - If the source dataset is not found on MinIO

    Returns
    -------
    None
    """
    logger.info(f"IGN AdminExpress {year} {borders=}")
    try:
        if not year:
            year = date.today().year
        elif year == "*":
            raise ValueError(f"Cannot use a * wildcard on {year=}")

        dataset_family = "ADMINEXPRESS"
        source = "EXPRESS-COG-TERRITOIRE"
        provider = "IGN"
        pattern = (
            f"{bucket}/{path_within_bucket}/"
            f"{provider=}/{dataset_family=}/{source=}/{year=}/**/{territory=}"
            f"/**/{borders}.shp"
        ).replace("'", "")
        files = fs.glob(pattern)  # , refresh=True)
        # see issue : https://github.com/fsspec/s3fs/issues/504
        if not files:
            raise ValueError(
                "No file retrieved with the set parameters, resulting to the "
                f"following {pattern=}"
            )

        data = []
        for file in files:
            logger.info(f"retrieving {file=}")
            with tempfile.TemporaryDirectory() as tempdir:
                pattern = file.rsplit(".", maxsplit=1)[0]
                all_files = fs.glob(pattern + ".*")  # , refresh=True)
                # see issue : https://github.com/fsspec/s3fs/issues/504
                for temp in all_files:
                    with open(
                        os.path.join(tempdir, os.path.basename(temp)), "wb"
                    ) as tf:
                        with fs.open(temp, "rb") as fsf:
                            tf.write(fsf.read())
                gdf = gpd.read_file(
                    os.path.join(tempdir, os.path.basename(file))
                )

            gdf = gdf.to_crs(4326)
            data.append(gdf)
        gdf = gpd.pd.concat(data)

        if borders == "ARRONDISSEMENT_MUNICIPAL":
            gdf["INSEE_DEP"] = gdf["INSEE_COM"].str[:2]

        gdf["source"] = f"{provider}:{source}"
        gdf["source_geometries"] = f"{provider}:{source}"

        config_dict = {
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "year": year,
            "borders": borders,
            "crs": gdf.crs.to_epsg(),
            "filter_by": "origin",
            "value": "preprocessed",
            "file_format": "GPKG",
            "provider": "cartiflette",
            "dataset_family": dataset_family,
            "source": source,
            "territory": territory if territory != "*" else "france_entiere",
            "filename": f"{borders}.gpkg",
            "simplification": 0,
        }
        path = create_path_bucket(config=config_dict)
        with fs.open(path, "wb") as f:
            gdf.to_file(f, driver="GPKG", encoding="utf8")
    except Exception as e:
        logger.error(e)


def store_vectorfile_communes_arrondissement(
    year: int = None,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
) -> None:
    """
    Store "enriched" dataframe for cities, using also cities' districts.

    Parameters
    ----------
    year : int, optional
        Desired vintage. Will use the current year if set to None (which is
        default).
    bucket : str, optional
        Bucket to use. The default is BUCKET.
    path_within_bucket : str, optional
        path within bucket. The default is PATH_WITHIN_BUCKET.
    fs : s3fs.S3FileSystem, optional
        S3 file system to use. The default is FS.
    Returns
    -------
    None


    """

    if not year:
        year = date.today().year

    kwargs = {
        "year": year,
        "crs": "4326",
        "filter_by": "origin",
        "value": "preprocessed",
        "file_format": "GPKG",
        "provider": "cartiflette",
        "source": "EXPRESS-COG-TERRITOIRE",
        "dataset_family": "ADMINEXPRESS",
        "territory": "france_entiere",
        "type_download": "bucket",
        "fs": fs,
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
    }

    try:
        arrondissements = download_file_single(
            borders="ARRONDISSEMENT_MUNICIPAL",
            filename="ARRONDISSEMENT_MUNICIPAL.gpkg",
            **kwargs,
        )

        communes = download_file_single(
            borders="COMMUNE",
            filename="COMMUNE.gpkg",
            **kwargs,
        )
    except Exception as e:
        logger.error(e)

    field = "NOM"
    try:
        communes[field]
    except KeyError:
        field = "NOM_COM"
    communes_sans_grandes_villes = communes.loc[
        ~communes[field].isin(["Marseille", "Lyon", "Paris"])
    ]
    communes_grandes_villes = communes.loc[
        communes[field].isin(["Marseille", "Lyon", "Paris"])
    ]

    arrondissement_extra_info = arrondissements.merge(
        communes_grandes_villes, on="INSEE_DEP", suffixes=("", "_y")
    )
    arrondissement_extra_info = arrondissement_extra_info.loc[
        :, ~arrondissement_extra_info.columns.str.endswith("_y")
    ]

    gdf_enrichi = pd.concat(
        [communes_sans_grandes_villes, arrondissement_extra_info]
    )

    try:
        field = "INSEE_ARM"
        gdf_enrichi[field]
    except KeyError:
        field = "INSEE_RATT"
    gdf_enrichi["INSEE_COG"] = np.where(
        gdf_enrichi[field].isnull(),
        gdf_enrichi["INSEE_COM"],
        gdf_enrichi[field],
    )

    gdf_enrichi = gdf_enrichi.drop(field, axis="columns")

    try:
        config_dict = {
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "year": year,
            "borders": "COMMUNE_ARRONDISSEMENTS_MUNICIPAUX",
            "crs": gdf_enrichi.crs.to_epsg(),
            "filter_by": "origin",
            "value": "preprocessed",
            "file_format": "GPKG",
            "provider": "cartiflette",
            "dataset_family": "ADMINEXPRESS",
            "source": "COMMUNE_ARRONDISSEMENTS_MUNICIPAUX",
            "territory": "france_entiere",
            "filename": "COMMUNE_ARRONDISSEMENTS_MUNICIPAUX.gpkg",
            "simplification": 0,
        }
        path = create_path_bucket(config=config_dict)
        with fs.open(path, "wb") as f:
            gdf_enrichi.to_file(f, driver="GPKG", encoding="utf8")
    except Exception as e:
        logger.error(e)


def store_living_area(
    year: int = None,
    bv_source: str = "FondsDeCarte_BV_2022",
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
) -> gpd.GeoDataFrame:
    """
    Reconstruct living areas ("Bassins de vie") from AdminExpress' cities' 
    geometries and Insee's inventory.

    Parameters
    ----------
    year : int, optional
        Desired vintage. Will use the current year if set to None (which is
        default).
    bv_source : str, optional
        Dataset's source to use for living area. The default is 
        "FondsDeCarte_BV_2022".
    bucket : str, optional
        Bucket to use. The default is BUCKET.
    path_within_bucket : str, optional
        path within bucket. The default is PATH_WITHIN_BUCKET.
    fs : s3fs.S3FileSystem, optional
        S3 file system to use. The default is FS.

    Raises
    ------
    ValueError
        If no file has been found on S3 for the given parameters.

    Returns
    -------
    bv : gpd.GeoDataFrame
        GeoDataFrame of living areas, constructed from cities geometries

          Ex.:
              bv              libbv dep reg  \
        0  01004  Ambérieu-en-Bugey  01  84
        1  01033         Valserhône  01  84
        2  01033         Valserhône  74  84
        3  01034             Belley  01  84
        4  01053    Bourg-en-Bresse  01  84

                                                    geometry  POPULATION
        0  POLYGON ((5.31974 45.92194, 5.31959 45.92190, ...       46645
        1  POLYGON ((5.72192 46.03413, 5.72165 46.03449, ...       25191
        2  POLYGON ((5.85098 45.99099, 5.85094 45.99070, ...        4566
        3  POLYGON ((5.61963 45.66754, 5.61957 45.66773, ...       25620
        4  POLYGON ((5.18709 46.05114, 5.18692 46.05085, ...       83935

    """

    if not year:
        year = date.today().year

    try:
        territory = "france_entiere"
        provider = "Insee"
        dataset_family = "BV"
        source = bv_source
        pattern = (
            f"{bucket}/{path_within_bucket}/"
            f"{provider=}/{dataset_family=}/{source=}/{year=}/**/{territory=}"
            f"/**/*.dbf"
        ).replace("'", "")
        files = fs.glob(pattern)  # , refresh=True)
        # see issue : https://github.com/fsspec/s3fs/issues/504
        if not files:
            raise ValueError(
                "No file retrieved with the set parameters, resulting to the "
                f"following {pattern=}"
            )
        data = []
        for file in files:
            with tempfile.TemporaryDirectory() as tempdir:
                tmp_dbf = os.path.join(tempdir, os.path.basename(file))
                with open(tmp_dbf, "wb") as tf:
                    with fs.open(file, "rb") as fsf:
                        tf.write(fsf.read())

                df = gpd.read_file(tmp_dbf, encoding="utf8")
                df = df.drop("geometry", axis=1)
            data.append(df)

        bv = pd.concat(data)

        kwargs = {
            "year": year,
            "crs": "4326",
            "filter_by": "origin",
            "value": "preprocessed",
            "file_format": "GPKG",
            "provider": "cartiflette",
            "source": "EXPRESS-COG-TERRITOIRE",
            "dataset_family": "ADMINEXPRESS",
            "territory": "france_entiere",
            "type_download": "bucket",
            "fs": fs,
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
        }
        communes = download_file_single(
            borders="COMMUNE",
            filename="COMMUNE.gpkg",
            **kwargs,
        )

        bv = communes.merge(
            bv, left_on="INSEE_COM", right_on="codgeo", how="right"
        )
        bv["source"] = f"{provider}:{source}"

        if bv_source == "FondsDeCarte_BV_2022":
            rename = ["bv2022", "libbv2022"]
        elif bv_source == "FondsDeCarte_BV_2012":
            rename = ["bv2012", "libbv2012"]
        bv = bv.rename(dict(zip(rename, ["bv", "libbv"])), axis=1)

        config_dict = {
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "year": year,
            "borders": "COMMUNE",
            "crs": bv.crs.to_epsg(),
            "filter_by": "origin",
            "value": "preprocessed",
            "file_format": "GPKG",
            "provider": "cartiflette",
            "dataset_family": f"bassins-vie-{bv_source.split('_')[-1]}",
            "source": "BV",
            "filename": "bassins_vie.gpkg",
            "simplification": 0,
        }
        path = create_path_bucket(config=config_dict)
        with fs.open(path, "wb") as f:
            bv.to_file(f, driver="GPKG")

        by = ["bv", "libbv", "dep", "reg"]

        bv = bv.dissolve(
            by=by, aggfunc={"POPULATION": "sum"}, as_index=False, dropna=False
        )

        config_dict = {
            "bucket": bucket,
            "path_within_bucket": path_within_bucket,
            "year": year,
            "borders": "BASSIN-VIE",
            "crs": bv.crs.to_epsg(),
            "filter_by": "origin",
            "value": "preprocessed",
            "file_format": "GPKG",
            "provider": "cartiflette",
            "dataset_family": f"bassins-vie-{bv_source.split('_')[-1]}",
            "source": "BV",
            "territory": "france_entiere",
            "filename": "bassins_vie.gpkg",
            "simplification": 0,
        }
        path = create_path_bucket(config=config_dict)
        with fs.open(path, "wb") as f:
            bv.to_file(f, driver="GPKG")

        return

    except Exception as e:
        logger.error(e)


def preprocess_one_year(
    year: int,
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
):
    # TODO : docstring, preprocessing par année à cause des dépendances
    # entre jeux de données
    kwargs = {
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "fs": fs,
    }
    logger.info(f"Insee COG {year=}")
    store_cog_year(year=year, **kwargs)

    # Concaténation d'AdminExpress Communal (et formatage identique des
    # arrondissements municipaux pour reprojection / ajout source en vue
    # de l'étape de préparation du mix communes/arrondissements)

    func = partial(store_cog_ign, year=year, territory="*", **kwargs)
    if THREADS_DOWNLOAD > 1:
        with ThreadPool(2) as pool:
            iterator = pool.map(
                func, ["COMMUNE", "ARRONDISSEMENT_MUNICIPAL"]
            ).result()
            while True:
                try:
                    next(iterator)
                except StopIteration:
                    break
                except Exception as e:
                    logger.error(e)
    else:
        for source in [
            "COMMUNE",
            "ARRONDISSEMENT_MUNICIPAL",
        ]:
            store_cog_ign(
                year=year,
                territory="*",
                borders=source,
                **kwargs,
            )

    # Préparation des AdminExpress enrichis avec arrondissements municipaux
    logger.info(
        f"IGN AdminExpress mixup {year} , "
        "sources=['COMMUNE', 'ARRONDISSEMENT_MUNICIPAL']"
    )
    store_vectorfile_communes_arrondissement(year=year, **kwargs)

    if year >= 2022:
        logger.info(f"INSEE living area v2022 in {year}")
        store_living_area(year, "FondsDeCarte_BV_2022", **kwargs)

    if 2012 <= year <= 2022:
        logger.info(f"INSEE living area v2012 in {year}")
        store_living_area(year, "FondsDeCarte_BV_2012", **kwargs)


def preprocess_pipeline(
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
):
    kwargs = {
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
        "fs": fs,
    }

    years = list(range(2015, date.today().year + 1))[-1::-1]

    logger.info("Preprocess raw sources")

    if THREADS_DOWNLOAD > 1:
        with ThreadPool(THREADS_DOWNLOAD) as pool:
            iterator = pool.map(preprocess_one_year, years).result()
            while True:
                try:
                    next(iterator)
                except StopIteration:
                    break
                except Exception as e:
                    logger.error(e)

    else:
        for year in years:
            preprocess_one_year(year, **kwargs)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # preprocess_pipeline()
    store_living_area(year=2022)
