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
from shapely.geometry import Polygon
import sqlite3
import tempfile


from cartiflette import BUCKET, PATH_WITHIN_BUCKET, FS, THREADS_DOWNLOAD
from cartiflette.utils import magic_csv_reader, create_path_bucket
from cartiflette.public import download_file_single

logger = logging.getLogger(__name__)

# TODO : docstrings


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

        by = ["bv", "libbv"]

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


def preprocess_banatic(
    year: int = None,
    bv_source: str = "FondsDeCarte_BV_2022",
    bucket: str = BUCKET,
    path_within_bucket: str = PATH_WITHIN_BUCKET,
    fs: s3fs.S3FileSystem = FS,
) -> gpd.GeoDataFrame:
    # Récupération du fond de carte
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
        "type_download": "https",  # TODO : change
        "fs": fs,
        "bucket": bucket,
        "path_within_bucket": path_within_bucket,
    }
    communes = download_file_single(
        borders="COMMUNE",
        filename="COMMUNE.gpkg",
        **kwargs,
    )
    communes = communes[["INSEE_COM", "geometry"]]
    #   INSEE_COM                                           geometry
    # 0     43076  POLYGON ((3.61424 45.27211, 3.61381 45.27253, ...
    # 1     65462  POLYGON ((-0.02484 43.43001, -0.02502 43.43012...
    # 2     29058  MULTIPOLYGON (((-3.97907 47.70396, -3.97909 47...
    # 3     22218  MULTIPOLYGON (((-3.20269 48.84901, -3.20267 48...
    # 4     38250  POLYGON ((5.43224 45.61478, 5.43223 45.61463, ...

    # Récupération de la table de liaison n°INSEE / n°SIREN
    provider = "MinistereInterieur"
    dataset_family = "BANATIC"
    source = "CORRESPONDANCE-SIREN-INSEE"
    pattern = (
        f"{bucket}/{path_within_bucket}/"
        f"{provider=}/{dataset_family=}/{source=}/{year=}/**/*.xlsx"
    ).replace("'", "")
    files = fs.glob(pattern)  # , refresh=True)
    if not files:
        raise ValueError(
            "No file retrieved with the set parameters, resulting to the "
            f"following {pattern=}"
        )
    file = files[0]
    logger.info(f"download {file=}")
    with fs.open(file, "rb") as f:
        dummy = io.BytesIO(f.read())
    siren_insee = pd.read_excel(dummy, dtype={"siren": str, "insee": str})
    siren_insee = siren_insee[["siren", "insee"]].rename(
        {"siren": "SIREN", "insee": "INSEE_COM"}, axis=1
    )
    #        SIREN INSEE_COM
    # 0  210100012     01001
    # 1  210100020     01002
    # 2  210100046     01004
    # 3  210100053     01005
    # 4  210100061     01006

    # Récupération des données BANATIC
    banatic = []
    source = "PERIMETRE-GROUPEMENTS"
    # projet-cartiflette/diffusion/test-demoday/provider=MinistereInterieur/dataset_family=BANATIC/source=PERIMETRE-GROUPEMENTS/year=2022/administrative_level=None/crs=None/origin=raw/file_format=None/territory=Auvergne-Rhone-Alpes/simplification=0/DATASET_MINISTEREINTERIEUR_BANATIC_PERIMETRE_GROUPEMENTS_AUVERGNE_RHONE_ALPES_2022.csv
    pattern = (
        f"{bucket}/{path_within_bucket}/"
        f"{provider=}/{dataset_family=}/{source=}/{year=}/**/*.csv"
    ).replace("'", "")
    files = fs.glob(pattern)  # , refresh=True)
    # see issue : https://github.com/fsspec/s3fs/issues/504
    if not files:
        raise ValueError(
            "No file retrieved with the set parameters, resulting to the "
            f"following {pattern=}"
        )
    for file in files:
        logger.info(f"download {file=}")
        with fs.open(file, "rb") as f:
            dummy = io.BytesIO(f.read())
        df = magic_csv_reader(
            dummy, dtype={"N° SIREN": str, "Siren membre": str}
        )
        banatic.append(df)

    banatic = pd.concat(banatic, ignore_index=True)
    rename = {
        "N° SIREN": "SIREN",
        "Nom du groupement": "NOM",
        "Nature juridique": "TYPE",
        "Syndicat à la carte": "A_LA_CARTE",
        "Groupement interdépartemental": "INTER_DEP",
        "Zone de montagne": "MONTAGNE",
        "Date de création": "ARRETE_CREATION",
        "Date d'effet": "DATE_EFFET_CREATION",
        "Mode de répartition des sièges": "REPARTITION_SIEGES",
        "Autre mode de répartition des sièges": "AUTRE_REPARTITION",
        "Nombre de membres": "NB_MEMBRES",
        "Population": "POPULATION",
        "Nombre de compétences exercées": "NB_COMPETENCES",
        "Mode de financement": "MODE_FINANCEMENT",
        "DGF Bonifiée": "ELIGIBLE_DGF",
        "DSC": "DSC_INSTITUEE",
        "REOM": "PERCEPTION_REOM",
        "Autre redevance": "PERCEPTION_AUTRE_REDEVANCE",
        "TEOM": "PERCEPTION_TEOM",
        "Autre taxe": "PERCEPTION_AUTRE_TAXE",
        "Civilité Président": "CIVILITE_PRESIDENT",
        "Prénom Président": "PRENOM_PRESIDENT",
        "Nom Président": "NOM_PRESIDENT",
        "Type": "MEMBRE_TYPE",
        "Siren membre": "MEMBRE_SIREN",
        "Nom membre": "MEMBRE_NOM",
        "Représentation-substitution": "MEMBRE_REPR_SUBST",
        "Compétence conservée": "MEMBRE_COMP_CONSERV",
        "C1004": "C1004",  # "Concession de la distribution d'électricité et de gaz"
        "C1010": "C1010",  # "Hydraulique"
        "C1015": "C1015",  # "Production, distribution d'énergie (OBSOLETE)"
        "C1020": "C1020",  # "Création, aménagement, entretien et gestion des réseaux de chaleur ou de froid urbains"
        "C1025": "C1025",  # "Soutien aux actions de MDE"
        "C1030": "C1030",  # "Autres énergies"
        "C1502": "C1502",  # "Eau (Traitement, Adduction, Distribution)"
        "C1505": "C1505",  # "Assainissement collectif"
        "C1507": "C1507",  # "Assainissement non collectif"
        "C1510": "C1510",  # "Collecte et traitement des déchets des ménages et déchets assimilés"
        "C1515": "C1515",  # "Traitement des déchets des ménages et déchets assimilés (OBSOLETE)"
        "C1520": "C1520",  # "Lutte contre les nuisances sonores"
        "C1525": "C1525",  # "Lutte contre la pollution de l'air"
        "C1528": "C1528",  # "GEMAPI : Aménagement d'un bassin ou d'une fraction de bassin hydrographique"
        "C1529": "C1529",  # "GEMAPI : Entretien et aménagement d'un cours d'eau, canal, lac ou plan d'eau"
        "C1530": "C1530",  # "Gestion des milieux aquatiques et prévention des inondations (GEMAPI) (OBSOLETE)"
        "C1531": "C1531",  # "GEMAPI : Défense contre les inondations et contre la mer"
        "C1532": "C1532",  # "GEMAPI : Protection et restauration des sites, des écosystèmes aquatiques, des zones humides et des formations boisées riveraines"
        "C1533": "C1533",  # "Gestion des eaux pluviales urbaines"
        "C1534": "C1534",  # "Maîtrise des eaux pluviales et de ruissellement ou la lutte contre l'érosion de sols"
        "C1535": "C1535",  # "Parcs naturels régionaux"
        "C1540": "C1540",  # "Autres actions environnementales"
        "C1545": "C1545",  # "Autorité concessionnaire de l'Etat pour les plages, dans les conditions prévues à l'article L. 2124-4 du code général de la propriété des personnes publiques. "
        "C1550": "C1550",  # "Création et entretien des infrastructures de charge nécessaires à l'usage des véhicules électriques ou hybrides rechargeables, en application de l'article L. 2224-37 du CGCT. "
        "C1555": "C1555",  # "Elaboration et adoption du plan climat-air-énergie territorial en application de l'article L. 229-26 du code de l'environnement"
        "C1560": "C1560",  # " Contribution à la transition énergétique "
        "C2005": "C2005",  # "Création, gestion , extension et translation des cimetières et sites funéraires"
        "C2010": "C2010",  # "Création, gestion et extension des crématoriums et sites cinéraires"
        "C2015": "C2015",  # "Service extérieur de Pompes funèbres"
        "C2510": "C2510",  # "Aide sociale facultative"
        "C2515": "C2515",  # "Activités sanitaires"
        "C2520": "C2520",  # "Action sociale"
        "C2521": "C2521",  # "Crèche, Relais assistance maternelle, aide à la petite enfance"
        "C2525": "C2525",  # "CIAS"
        "C2526": "C2526",  # "Maisons de santé pluridisciplinaires"
        "C3005": "C3005",  # "Elaboration du diagnostic du territoire et définition des orientations du contrat de ville ; animation et coordination des dispositifs contractuels de développement urbain, de développement local et d'insertion économique et sociale ainsi que des dispositifs locaux de prévention de la délinquance ; programmes d'actions définis dans le contrat de ville"
        "C3010": "C3010",  # "Contrat local de sécurité transports"
        "C3015": "C3015",  # "PLIE (plan local pour l'insertion et l'emploi) (OBSOLETE)"
        "C3020": "C3020",  # "CUCS (contrat urbain de cohésion sociale) (OBSOLETE)"
        "C3025": "C3025",  # "Rénovation urbaine (ANRU) (OBSOLETE)"
        "C3210": "C3210",  # "Conseil intercommunal de sécurité et de prévention de la délinquance (OBSOLETE)"
        "C3220": "C3220",  # "Contrat local de sécurité transports (OBSOLETE)"
        "C3505": "C3505",  # "Actions de développement économique dans les conditions prévues à l'article L. 4251-17 ; création, aménagement, entretien et gestion de zones d'activité industrielle, commerciale, tertiaire, artisanale, touristique, portuaire ou aéroportuaire ; politique locale du commerce et soutien aux activités commerciales "
        "C3510": "C3510",  # "Création, aménagement, entretien et gestion de zone d’activités portuaire ou aéroportuaire (OBSOLETE)"
        "C3515": "C3515",  # "Action de développement économique (soutien des activités industrielles, commerciales ou de l'emploi, soutien des activités agricoles et forestières…) (OBSOLETE)"
        "C4005": "C4005",  # "Construction ou aménagement, entretien, gestion d’équipements ou d’établissements culturels, socioculturels, socio-éducatifs, sportifs (OBSOLETE)"
        "C4006": "C4006",  # "Construction, aménagement, entretien, gestion d'équipements ou d'établissements culturels, socio-culturels, socio-éducatifs (OBSOLETE)"
        "C4007": "C4007",  # "Construction, aménagement, entretien, gestion d'équipements ou d'établissements sportifs (OBSOLETE)"
        "C4008": "C4008",  # "Construction, aménagement, entretien et gestion d'équipements culturels et sportifs"
        "C4010": "C4010",  # "Construction, aménagement, entretien et fonctionnement d'équipements de l'enseignement pré-élementaire et élémentaire"
        "C4015": "C4015",  # "Activités péri-scolaires"
        "C4016": "C4016",  # "Lycées et collèges "
        "C4017": "C4017",  # "Programme de soutien et d'aides aux établissements d'enseignement supérieur et de recherche et aux programmes de recherche"
        "C4020": "C4020",  # "Activités culturelles ou socioculturelles"
        "C4025": "C4025",  # "Activités sportives"
        "C4505": "C4505",  # "Schéma de cohérence territoriale (SCOT)"
        "C4510": "C4510",  # "Schéma de secteur"
        "C4515": "C4515",  # "Plans locaux d’urbanisme"
        "C4520": "C4520",  # "Création et réalisation de zone d’aménagement concertée (ZAC)"
        "C4525": "C4525",  # "Constitution de réserves foncières"
        "C4530": "C4530",  # "Organisation de la mobilité, au sens des articles L.1231-1 et suivants du code des transports"
        "C4531": "C4531",  # "Transports scolaires"
        "C4532": "C4532",  # "Organisation des transports non urbains"
        "C4535": "C4535",  # "Prise en considération d’un programme d’aménagement d’ensemble et détermination des secteurs d’aménagement au sens du code de l’urbanisme"
        "C4540": "C4540",  # "Voies navigables et ports intérieurs (OBSOLETE)"
        "C4545": "C4545",  # "Aménagement rural (OBSOLETE)"
        "C4550": "C4550",  # "Plans de déplacement urbains"
        "C4555": "C4555",  # "Études et programmation"
        "C4560": "C4560",  # "Délivrance des autorisations d'occupation du sol (Permis de construire...)"
        "C5005": "C5005",  # "Création, aménagement, entretien de la voirie"
        "C5010": "C5010",  # "Signalisation"
        "C5015": "C5015",  # "Parcs de stationnement"
        "C5210": "C5210",  # "Promotion du tourisme dont la création d'offices de tourisme"
        "C5220": "C5220",  # "Thermalisme"
        "C5505": "C5505",  # "Programme local de l’habitat"
        "C5510": "C5510",  # "Politique du logement non social"
        "C5515": "C5515",  # "Politique du logement social"
        "C5520": "C5520",  # "Politique du logement étudiant"
        "C5525": "C5525",  # "Action et aide financière en faveur du logement social"
        "C5530": "C5530",  # "Action en faveur du logement des personnes défavorisées"
        "C5535": "C5535",  # "Opération programmée d'amélioration de l'habitat (OPAH)"
        "C5540": "C5540",  # "Amélioration du parc immobilier bâti "
        "C5545": "C5545",  # "Droit de préemption urbain (DPU) pour la mise en œuvre de la politique communautaire d'équilibre social de l'habitat"
        "C5550": "C5550",  # "Actions de réhabilitation et résorption de l’habitat insalubre"
        "C5555": "C5555",  # "Délégations des aides à la pierre (article 61 - Loi LRL)"
        "C7005": "C7005",  # "Ports"
        "C7010": "C7010",  # "Aérodromes"
        "C7012": "C7012",  # "Participation à la gouvernance et à l'aménagement des gares situées sur le territoire métropilitain"
        "C7015": "C7015",  # "Voies navigables"
        "C7020": "C7020",  # "Eclairage public"
        "C7025": "C7025",  # "Pistes cyclables"
        "C7030": "C7030",  # "Abattoirs, abattoirs-marchés et marchés d'intérêt national, halles, foires"
        "C9905": "C9905",  # "Préparation et réalisation des enquêtes de recensement de la population"
        "C9910": "C9910",  # "Préfiguration et fonctionnement des Pays"
        "C9915": "C9915",  # "Gestion de personnel (policiers-municipaux et garde-champêtre...)"
        "C9920": "C9920",  # "Acquisition en commun de matériel"
        "C9922": "C9922",  # "Gestion d'un centre de secours"
        "C9923": "C9923",  # "Service public de défense extérieure contre l'incendie"
        "C9924": "C9924",  # "Collecte des contributions pour le financement du SDIS"
        "C9925": "C9925",  # "Infrastructure de télécommunication (téléphonie mobile...)"
        "C9930": "C9930",  # "NTIC (Internet, câble…)"
        "C9935": "C9935",  # "Aménagement, entretien et gestion des aires d'accueil des gens du voyage"
        "C9940": "C9940",  # "Création et gestion des maisons de services au public"
        "C9950": "C9950",  # "Archives"
        "C9999": "C9999",  # "Autres"
    }
    banatic = banatic.rename(rename, axis=1)
    drop = [x for x in banatic.columns.tolist() if x not in rename.values()]
    banatic = banatic.drop(drop, axis=1)

    membership = banatic[
        [
            "SIREN",
            "MEMBRE_TYPE",
            "MEMBRE_SIREN",
            "MEMBRE_NOM",
            "MEMBRE_REPR_SUBST",
            "MEMBRE_COMP_CONSERV",
        ]
    ]
    membership = membership.dropna()

    #        SIREN MEMBRE_SIREN MEMBRE_NOM  MEMBRE_REPR_SUBST  MEMBRE_COMP_CONSERV
    # 0  200042935    210100111   Apremont                  0                    0
    # 1  200042935    210100129      Aranc                  0                    0
    # 2  200042935    210100145     Arbent                  0                    0

    banatic = banatic.drop(
        [
            "MEMBRE_SIREN",
            "MEMBRE_NOM",
            "MEMBRE_REPR_SUBST",
            "MEMBRE_COMP_CONSERV",
        ],
        axis=1,
    )
    banatic = banatic.drop_duplicates()

    #          SIREN                              NOM TYPE  A_LA_CARTE  INTER_DEP  \
    # 0    200042935       Haut - Bugey Agglomération   CA           0          0
    # 42   200071751  CA du Bassin de Bourg-en-Bresse   CA           0          0
    # 116  240100750                CA du Pays de Gex   CA           0          0

    #     ARRETE_CREATION DATE_EFFET_CREATION REPARTITION_SIEGES AUTRE_REPARTITION  \
    # 0        2014-01-01          2014-01-01                RDC               NaN
    # 42       2016-12-16          2017-01-01                RDC               NaN
    # 116      1995-05-31          1995-05-31                RDC               NaN

    #      NB_MEMBRES  POPULATION  NB_COMPETENCES MODE_FINANCEMENT  ELIGIBLE_DGF  \
    # 0            42       65190              39              FPU             0
    # 42           74      137011              44              FPU             0
    # 116          27      100515              39              FPU             0

    #      DSC_INSTITUEE  PERCEPTION_REOM PERCEPTION_AUTRE_REDEVANCE  \
    # 0                0                0                        NaN
    # 42               0                1                        NaN
    # 116              0                0

    #      PERCEPTION_TEOM PERCEPTION_AUTRE_TAXE CIVILITE_PRESIDENT  \
    # 0                  0                   NaN                 M.
    # 42                 1                   NaN                 M.
    # 116                0                                       M.

    #     PRENOM_PRESIDENT NOM_PRESIDENT  C1004  C1010  C1015  C1020  C1025  C1030  \
    # 0               Jean      DEGUERRY      0      0      0      0      0      0
    # 42     Jean-François         DEBAT      0      0      0      0      1      0
    # 116          Patrice        DUNAND      0      0      0      1      0      0

    # ...

    #      C9922  C9923  C9924  C9925  C9930  C9935  C9940  C9950  C9999
    # 0        0      0      0      0      0      1      1      0      1
    # 42       0      0      1      0      0      1      0      0      0
    # 116      0      0      0      0      0      1      1      0      1

    # Préparation des géométries des communes avec code SIREN
    geoms = siren_insee.merge(communes, on="INSEE_COM", how="left")
    geoms = geoms.drop("INSEE_COM", axis=1)

    # %%
    geoms_ok = []
    temp_geoms = membership.merge(
        geoms, left_on="MEMBRE_SIREN", right_on="SIREN", how="left"
    )
    # TODO : supprimer départements, régions, autres, etc.

    # %%
    # TODO
    while not temp_geoms.empty:
        # %%
        # Nota : manque le cas des syndicats à géométrie "vide" :
        # ex. 200009579
        #           SIREN      MEMBRE_TYPE MEMBRE_SIREN                   MEMBRE_NOM  \
        # 3654  200009579  Autre organisme    220700017     Département de l'Ardèche
        # 3655  200009579  Autre organisme    200053767  Région Auvergne-Rhône-Alpes

        #       MEMBRE_REPR_SUBST  MEMBRE_COMP_CONSERV
        # 3654                  0                    0
        # 3655                  0                    0

        ix = temp_geoms[temp_geoms.MEMBRE_TYPE == "Autre organisme"].index
        # temp_geoms = temp_geoms.drop(ix)
        temp_geoms.loc[ix, "geometry"] = Polygon()

        temp_geoms = temp_geoms.drop("SIREN_y", axis=1)
        temp_geoms = temp_geoms.rename({"SIREN_x": "SIREN"}, axis=1)
        geoms_collected = (
            temp_geoms[["SIREN", "geometry"]]
            .dropna()
            .groupby("SIREN")["geometry"]
            .count()
        )
        geoms_collected.name = "GEOMS_COLLECTED"
        geoms_target = (
            temp_geoms[["SIREN", "MEMBRE_SIREN"]]
            .groupby("SIREN")["MEMBRE_SIREN"]
            .count()
        )
        geoms_target.name = "GEOMS_TARGET"
        temp_geoms = temp_geoms.merge(
            geoms_collected, how="left", on="SIREN"
        ).merge(geoms_target, on="SIREN", how="left")

        ix = temp_geoms[
            temp_geoms.GEOMS_COLLECTED == temp_geoms.GEOMS_TARGET
        ].index
        temp_geoms["GEOM_SAFE"] = 0
        temp_geoms.loc[ix, "GEOM_SAFE"] = 1

        temp_geoms = temp_geoms.drop(
            ["GEOMS_TARGET", "GEOMS_COLLECTED"], axis=1
        )
        # print(temp_geoms.GEOM_SAFE.value_counts(normalize=True))

        ix = temp_geoms[temp_geoms.GEOM_SAFE == 1].index

        geoms_ok.append(
            temp_geoms.loc[ix, ["SIREN", "MEMBRE_SIREN", "geometry"]]
        )
        temp_geoms = temp_geoms.drop(ix).drop("geometry", axis=1)
        temp_geoms = temp_geoms.drop_duplicates(keep="first")

        temp_geoms = temp_geoms.merge(
            pd.concat(
                [
                    pd.concat(geoms_ok, ignore_index=True)
                    .drop_duplicates(["SIREN", "MEMBRE_SIREN"])
                    .drop("MEMBRE_SIREN", axis=1),
                    geoms,
                ],
                ignore_index=True,
            ),
            left_on="MEMBRE_SIREN",
            right_on="SIREN",
            how="left",
        )
        # print(temp_geoms.shape[0])
        # print(
        #     len(
        #         pd.concat(geoms_ok, ignore_index=True)
        #         .drop_duplicates(["SIREN", "MEMBRE_SIREN"])
        #         .drop("MEMBRE_SIREN", axis=1)
        #     )
        # )
        # %%

    geoms_groupements = pd.concat(geoms_ok, ignore_index=True).drop_duplicates(
        ["SIREN", "MEMBRE_SIREN"]
    )
    membership = membership.merge(
        geoms_groupements, on=["SIREN", "MEMBRE_SIREN"], how="left"
    )

    membership = gpd.GeoDataFrame(membership, crs=communes.crs)
    membership.to_file("banatic.gpkg", driver="GPKG", layer="perimetres")
    with sqlite3.connect("banatic.gpkg") as con:
        banatic.to_sql("collectivites", if_exists="replace", con=con)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # preprocess_pipeline()
    preprocess_banatic(year=2022)
