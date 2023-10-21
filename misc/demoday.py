# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 18:16:27 2023

@author: thomas.grandjean

Démo bassins de vie (Clôture du programme 10%)
"""
from codecarbon import track_emissions


# %% Utilisation de cartiflette
@track_emissions(
    project_name="living_area_cartiflette", country_iso_code="FRA"
)
def get_living_area_cartiflette():
    from cartiflette.public import get_BV

    gdf = get_BV(year=2022)
    return gdf


# %% Sans cartiflette
@track_emissions(project_name="living_area_insee", country_iso_code="FRA")
def get_living_area_insee():
    import geopandas as gpd
    from glob import glob
    import io
    import os
    import requests
    import zipfile

    s = requests.Session()
    url = "https://www.insee.fr/fr/statistiques/fichier/6676988/fonds_bv2022_2022.zip"
    r = s.get(url)
    o = io.BytesIO(r.content)
    with zipfile.ZipFile(o) as f:
        f.extract("com_bv2022_2022.zip", "./temp.zip")
    with zipfile.ZipFile("./temp.zip") as f:
        f.extractall(".")
    gdf = gpd.read_file("./com_bv2022_2022.shp")
    os.unlink("com_bv2022_2022.zip")
    files = glob("./com_bv2022_2022.*")
    for file in files:
        os.unlink(file)
    by = ["bv2022", "libbv2022", "dep", "reg"]
    gdf = gdf.dissolve(by, aggfunc={"POPULATION": "sum"}, as_index=False)
    # TODO : détection de l'encodage, qualité des géométries ?
    return gdf
