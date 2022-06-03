import os
import sys

import pandas as pd

sys.path.append(os.getcwd() + '/src/')
from utils import flatten, flatten_soft


def get_cohort(df: pd.DataFrame, id_customer: str, id_order: str,
               order_date: str, first_year=None) -> pd.DataFrame:
    """Fonction pour attribuer chaque client à une cohorte.

    Parameters
    ----------
    df : pd.DataFrame
        dataframe de base commandes à la maille ligne = article commandé
    id_customer : str
        champ d'identifiant client
    id_order : str
       champ d'identifiant commande
    order_date : str
        champ de date de commande
    first_year : _type_, optional
        année de début de l'analyse: tous les clients arrivés avant,
        seront groupés en une même cohorte, by default None

    Returns
    -------
    pd.DataFrame
        _description_
    """

    # Find the first command for each client
    df_cohort = df.sort_values(
        by=str(order_date),
        ascending=True).drop_duplicates(subset=str(id_customer),
                                        keep='first', inplace=False)
    # Keep only the year
    df_cohort['COHORT'] = pd.to_datetime(df_cohort[str(order_date)]).dt.year

    # Group together the cohorts born before the start of the study
    if first_year is not None:
        df_cohort['COHORT'] = df_cohort['COHORT'].apply(
            lambda x: str(
                'Before '+str(first_year-1)) if x < first_year else x)

    # Percolate the cohorts feature on the full dataframe
    df = pd.merge(df, df_cohort[[str(id_customer),'COHORT']],
                  on=str(id_customer), how='outer', suffixes=(False, False))
    return df


def get_order_number_per_customer(df: pd.DataFrame, id_customer: str, id_order: str) -> pd.DataFrame:
    """Fonction pour déterminer le nombre de commandes par client.

    Parameters
    ----------
    df : pd.DataFrame
        dataframe de base commandes à la maille ligne = articles
    id_customer : str
        champ d'identifiant client
    id_order : str
        champ d'identifiant commande

    Returns
    -------
    pd.DataFrame
        _description_
    """
    nb_order = flatten(df.groupby([str(id_customer)]).agg({str(id_order) : pd.Series.nunique}))
    nb_order.columns = [str(id_customer),'NB_ORDERS']
    df = pd.merge(df, nb_order[[str(id_customer),'NB_ORDERS']], on=str(id_customer), how='outer', suffixes=(False,False))
    return df


def get_order_universe(df: pd.DataFrame, id_order: str, order_date: str) -> pd.DataFrame:
    """Fonction pour déterminer l'univers majoritaire (l'univers désignant une catégorie de produits plus macro que la 
    maille produits) d'une commande.

    Parameters
    ----------
    df : pd.DataFrame
        dataframe de base commandes à la maille ligne = articles
    id_order : str
        champ d'identifiant commande
    order_date : str
        champ décrivant la catégorie d'un article

    Returns
    -------
    pd.DataFrame
        _description_
    """
    order_universe = df.groupby([str(id_order),str(id_universe)]).agg({str(value_order):sum}).reset_index().sort_values(by=str(value_order),ascending=False)
    order_universe = order_universe.drop_duplicates(subset=[str(id_order)],keep='first')
    order_universe.columns = [str(id_order),'MAIN_UNIVERSE',str(value_order)]
    df = pd.merge(df,order_universe[[str(id_order),'MAIN_UNIVERSE']],on=str(id_order),how='left',suffixes=(False,False))
    return df
