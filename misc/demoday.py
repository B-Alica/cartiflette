# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 18:16:27 2023

@author: thomas.grandjean

Démo bassins de vie (Clôture du programme 10%)
"""
from codecarbon import track_emissions

proxy = "http://pfrie-std.proxy.e2.rie.gouv.fr:8080"
proxy = ""
proxies = {"https": proxy, "http": proxy}


kwargs_cc = {
    "offline": True,
    "country_iso_code": "FRA",
}


# %% Sans cartiflette
def manual_tracker(use_cache=False):
    project_name = "living_area_insee"
    if not use_cache:
        project_name += "_without_cache"

    @track_emissions(project_name=project_name, **kwargs_cc)
    def get_living_area_insee():
        import geopandas as gpd
        import pandas as pd
        import numpy as np
        from glob import glob
        import io
        import os
        import requests_cache
        import zipfile

        # Création d'une session web
        s = requests_cache.CachedSession(
            "demoday_manual", expire_after=3600 * 24 * 2
        )

        # Configuration éventuelle des proxies au sein des ministères
        s.proxies.update(proxies)

        # Téléchargement et lecture des fonds de plan
        url = "https://www.insee.fr/fr/statistiques/fichier/6676988/fonds_bv2022_2022.zip"
        r = s.get(url)
        o = io.BytesIO(r.content)

        # Le contenu est un zip dans un zip...
        with zipfile.ZipFile(o) as f:
            f.extract("com_bv2022_2022.zip")
        with zipfile.ZipFile("com_bv2022_2022.zip") as f:
            f.extractall(".")
        gdf = gpd.read_file("./com_bv2022_2022.shp")

        # Nettoyage des fichiers temporaires liés au zip imbriqué
        os.unlink("com_bv2022_2022.zip")
        files = glob("./com_bv2022_2022.*")
        for file in files:
            os.unlink(file)

        # récupération des populations communales INSEE
        url = "https://www.insee.fr/fr/statistiques/fichier/2522602/fichier_poplegale_6820.xlsx"
        r = s.get(url)
        o = io.BytesIO(r.content)
        pop = pd.read_excel(
            o, sheet_name="2020", skiprows=[0, 1, 2, 3, 4, 5, 6]
        )
        pop = pop.rename({"COM": "codgeo", "PMUN20": "POPULATION"}, axis=1)

        # Mais les populations communales sont affectées aux arrondissements
        # municipaux alors que les bassins de vie utilisent les communes !
        dict_arrondissements_municipaux = {
            "13201": "13055",
            "13202": "13055",
            "13203": "13055",
            "13204": "13055",
            "13205": "13055",
            "13206": "13055",
            "13207": "13055",
            "13208": "13055",
            "13209": "13055",
            "13210": "13055",
            "13211": "13055",
            "13212": "13055",
            "13213": "13055",
            "13214": "13055",
            "13215": "13055",
            "13216": "13055",
            "69381": "69123",
            "69382": "69123",
            "69383": "69123",
            "69384": "69123",
            "69385": "69123",
            "69386": "69123",
            "69387": "69123",
            "69388": "69123",
            "69389": "69123",
            "75101": "75056",
            "75102": "75056",
            "75103": "75056",
            "75104": "75056",
            "75105": "75056",
            "75106": "75056",
            "75107": "75056",
            "75108": "75056",
            "75109": "75056",
            "75110": "75056",
            "75111": "75056",
            "75112": "75056",
            "75113": "75056",
            "75114": "75056",
            "75115": "75056",
            "75116": "75056",
            "75117": "75056",
            "75118": "75056",
            "75119": "75056",
            "75120": "75056",
        }
        ix = pop[pop.codgeo.isin(dict_arrondissements_municipaux)].index

        if len(ix) > 0:
            pop.loc[ix, "codgeo"] = pop.loc[ix, "codgeo"].map(
                dict_arrondissements_municipaux
            )
            pop = pop.groupby("codgeo")["POPULATION"].sum().astype(int)
            pop = pop.reset_index(drop=False)

        # Jointure des populations aux fonds de plan
        gdf = gdf.merge(pop[["codgeo", "POPULATION"]], on="codgeo", how="left")

        # Il manque la population de Mayotte qui obéit à des règles différentes
        # du reste du territoire : on recommence !
        url = "https://www.insee.fr/fr/statistiques/fichier/5392668/mayotte-RP2017-tableaux_pop_legale.xlsx"
        r = s.get(url)
        o = io.BytesIO(r.content)
        pop2 = pd.read_excel(o, sheet_name="Communes", skiprows=[0, 1])
        pop2.columns = ["Commune", "PopTotale", "PopMun", "PopAPart"]
        pop2 = pop2[:-1].drop(["PopTotale", "PopAPart"], axis=1)
        pop2["codgeo"] = np.nan
        pop2[["codgeo", "Commune"]] = pop2.Commune.str.split(
            " - ", expand=True
        )
        pop2["codgeo"] = "976" + pop2["codgeo"]
        pop2 = pop2.drop("Commune", axis=1)

        gdf = gdf.merge(pop2, on="codgeo", how="left")
        ix = gdf[(gdf.POPULATION.isnull()) & (gdf.PopMun.notnull())].index
        gdf.loc[ix, "POPULATION"] = gdf.loc[ix, "PopMun"]
        gdf = gdf.drop("PopMun", axis=1)

        gdf.POPULATION = gdf.POPULATION.astype(int)

        # On groupe par bassin de vie pour enfin calculer
        by = ["bv2022", "libbv2022"]
        gdf = gdf.dissolve(by, aggfunc={"POPULATION": "sum"}, as_index=False)

        return gdf

    return get_living_area_insee()


# %% Utilisation de cartiflette
def cartiflette_tracker(use_cache=False):
    project_name = "living_area_cartiflette"
    if not use_cache:
        project_name += "_without_cache"

    @track_emissions(project_name=project_name, **kwargs_cc)
    def get_living_area_cartiflette():
        from cartiflette.public import get_living_area

        if not use_cache:
            try:
                os.unlink("cartiflette.sqlite")
            except FileNotFoundError:
                pass
        # On télécharge et c'est tout (même avec un proxy)
        gdf = get_living_area(year=2022, year_bv=2022, proxies=proxies)
        return gdf

    return get_living_area_cartiflette()


def plot_results():
    legend_title = "Requêtes web"
    sns.set_style("darkgrid")
    df = pd.read_csv("emissions.csv")
    df = df[["project_name", "duration"]]
    df[legend_title] = df["project_name"].map(
        {
            "living_area_cartiflette": "avec cache",
            "living_area_insee": "avec cache",
            "living_area_cartiflette_without_cache": "sans cache",
            "living_area_insee_without_cache": "sans cache",
        }
    )
    df["project_name"] = df["project_name"].map(
        {
            "living_area_cartiflette": "Cartiflette",
            "living_area_insee": "Manuel",
            "living_area_cartiflette_without_cache": "Cartiflette",
            "living_area_insee_without_cache": "Manuel",
        }
    )

    f, ax = plt.subplots(figsize=(7, 6))

    kwargs = {
        "data": df,
        "x": "duration",
        "y": "project_name",
        "hue": legend_title,
    }

    ax = sns.swarmplot(alpha=0.5, zorder=1, **kwargs)
    ax.set(
        title="Comparaison des algorithmes",
    )

    ax.set(ylabel="")
    ax.set(xlabel="durée d'exécution (s)")

    # Improve the legend
    # leg = ax.legend()
    #
    sns.move_legend(
        ax,
        loc="lower right",
        # ncol=2,
        frameon=True,
        # columnspacing=1,
        handletextpad=0,
        alignment="center",
    )


if __name__ == "__main__":
    import geopandas as gpd
    import pandas as pd
    import numpy as np
    from glob import glob
    import io
    import os
    import requests_cache
    import zipfile
    from cartiflette.public import get_living_area
    import matplotlib.pyplot as plt
    import seaborn as sns

    # gdf = cartiflette_tracker(use_cache=False)
    # gdf.to_file("bv_cartiflette.gpkg", driver="GPKG")
    # print("-" * 50)
    # gdf = manual_tracker(use_cache=False)
    # gdf.to_file("bv_insee.gpkg", driver="GPKG")

    # for x in range(20):
    #     print("=" * 50)
    #     print(f"pass {x=} without cache")
    #     print("-" * 50)
    #     cartiflette_tracker(use_cache=False)
    #     print("-" * 50)
    #     manual_tracker(use_cache=False)
    # for x in range(20):
    #     print("=" * 50)
    #     print(f"pass {x=} with cache")
    #     print("-" * 50)
    #     cartiflette_tracker(use_cache=True)
    #     print("-" * 50)
    #     manual_tracker(use_cache=True)
    plot_results()

    # Tester un graph sur Villers-Cotterêts
