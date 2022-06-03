import os
import sys

import pandas as pd

sys.path.append(os.getcwd() + '/src/')
from utils import flatten


def build_waterfall(df: pd.DataFrame, id_customer: str, order_date: str,
                    value_order: str, id_cohort: str) -> pd.DataFrame:
    """
    Fonction pour construire le waterfall.

    Parameters
    ----------
    df : pd.DataFrame
        dataframe de base commandes à la maille ligne = article dans la commande
    id_customer : str
        champ d'identifiant client
    order_date : str
        champ de date de commande
    value_order : str
        champ de valeur de la commande (en €)
    id_cohort : str
        champ de cohorte qui peut être obtenu par get_cohort

    Returns
    -------
    pd.DataFrame
        Une fois sommé sur l'axe=0, les colonnes NEW_BUSINESS_XXXX, LFL_XXXX,
        CHURN_XXXX et REACTIVATED_XXXX permettent de tracer le waterfall
    """
    output = pd.DataFrame(columns=[str(id_customer), str(id_cohort)])

    df['YEAR_ORDER'] = df[str(order_date)].dt.year

    for year in df['YEAR_ORDER'].unique():
        df_tmp_peryear = df.loc[df.YEAR_ORDER == year]
        df_tmp_peryear = flatten(
            df_tmp_peryear.groupby([str(id_customer)])
            .agg({str(value_order): sum, str(id_cohort): 'last'}))
        df_tmp_peryear.columns =\
            [
                str(id_customer),
                str('SALES_'+str(year)),
                str('COHORT_'+str(year))
            ]
        df_tmp_peryear[str('ACTIVE_'+str(year))] = 1

        output = pd.merge(
            output,
            df_tmp_peryear,
            on=str(id_customer),
            how='outer',
            suffixes=(False, False))
        output[str(id_cohort)] = output[str(id_cohort)].combine_first(
            output[str('COHORT_' + str(year))])
        output.drop(columns=[str('COHORT_'+str(year))], inplace=True)

    for year in range(
            int(min(df[str(id_cohort)].unique())) + 1,
            int(max(df[str(id_cohort)].unique()))):
        output[str('NEW_BUSINESS_'+str(int(year+1)))] =\
            output.apply(
                lambda row: row[str('SALES_'+str(year+1))]
                if ((row[str('ACTIVE_'+str(year))] == 0)
                    and (row[str('ACTIVE_'+str(year+1))] == 1)
                    and row[str(id_cohort)] == year+1)
                else 0, axis=1)
        output[str('LFL_'+str(int(year)))] =\
            output.apply(
                lambda row: row[
                    str('SALES_'+str(year+1))] - row[str('SALES_'+str(year))]
                if ((row[str('ACTIVE_'+str(year))] == 1)
                    and (row[str('ACTIVE_'+str(year+1))] == 1))
                else 0, axis=1)
        output[str('CHURN_'+str(int(year+1)))] =\
            output.apply(
                lambda row: - row[str('SALES_'+str(year))]
                if ((row[str('ACTIVE_'+str(year))] == 1) 
                    and (row[str('ACTIVE_'+str(year+1))] == 0))
                else 0,
                axis=1)
        output[str('REACTIVATED_'+str(int(year+1)))] =\
            output.apply(
                lambda row: row[str('SALES_'+str(year+1))]
                if ((row[str('ACTIVE_'+str(year))] == 0)
                    and (row[str('ACTIVE_'+str(year+1))] == 1)
                    and (row[str(id_cohort)] < year+1))
                else 0, axis=1)

    return output
