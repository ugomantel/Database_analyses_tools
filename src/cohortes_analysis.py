from ast import Not
from msilib.schema import Error
import os
import sys 

import pandas as pd

sys.path.append(os.getcwd() + '/src/')
from utils import flatten, convert_column_to_float


def build_cohort_df(df: pd.DataFrame, id_order: str, order_date: str,
                    id_customer: str, id_cohort: str, value_order: str):
    """
    Fonction pour extraire la donnée en cohortes en valeur et volume.

    Parameters
    ----------
    df : pd.DataFrame
        dataframe de base commandes à la maille ligne = article dans la commande
    id_order : str
        champ d'identifiant commande
    order_date : str
        champ de date de commande
    id_customer : str
        champ d'identifiant client
    id_cohort : str
        champ de cohorte qui peut être obtenu par get_cohort
    value_order : str
        champ de valeur de la commande (en €)

    Returns
    -------
    _type_
        - Une fois groupé par cohortes, les colonnes ACTIVE_XXXX permettent d'obtenir les cohortes en volume 
        - Une fois groupé par cohortes, les colonnes SALES_XXXX permettent d'obtenir les cohortes en valeur 
    """
    output = pd.DataFrame(columns=[str(id_customer), str(id_cohort)])

    df['YEAR_ORDER'] = pd.to_datetime(df[str(order_date)]).dt.year

    for year in df['YEAR_ORDER'].unique():
        df_tmp_peryear = df.loc[df.YEAR_ORDER == year]
        # df_tmp_peryear[str(value_order)] =\
        #     df_tmp_peryear[str(value_order)].astype(float)
        # convert_column_to_float(df_tmp_peryear, str(value_order))
        df_tmp_peryear = flatten(
            df_tmp_peryear.groupby([str(id_customer)])
                          .agg(
                              {str(value_order): sum, str(id_cohort): 'last'}))
        df_tmp_peryear.columns = [
            str(id_customer),
            str('SALES_'+str(year)),
            str('COHORT_'+str(year))]
        df_tmp_peryear[str('ACTIVE_'+str(year))] = 1

        output = pd.merge(
            output, df_tmp_peryear, on=str(id_customer),
            how='outer', suffixes=(False, False))
        output[str(id_cohort)] = output[str(id_cohort)].\
            combine_first(output[str('COHORT_'+str(year))])
        output.drop(columns=[str('COHORT_'+str(year))], inplace=True)

    return output


def create_cohorts_analysis_table(cohorts_df: pd.DataFrame,
                                  analyse_type='sales') -> pd.DataFrame:
    if analyse_type == 'sales':
        interest_cols = [
            a for a in cohorts_df.columns if a.startswith('SALES_')]
    elif analyse_type == 'activity':
        interest_cols = [
            a for a in cohorts_df.columns if a.startswith('ACTIVE_')]
    else:
        raise Error('analyse_type should be one of "sales" or "activity')
    group_dict = dict([(key, sum) for key in interest_cols])
    df_by_cohorts = cohorts_df.groupby("COHORT").agg(group_dict)[interest_cols]
    return df_by_cohorts
