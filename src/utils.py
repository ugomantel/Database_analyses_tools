import logging

import pandas as pd


def flatten(table):
    """Flattens a dataframe grouped on a single column

    Parameters
    ----------
    table : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    if type(table.columns) == pd.MultiIndex:
        columns_to_look = [name_tmp for name_tmp in table.columns]

        columns_df = [str(t[0])+'_'+str(t[1]) for t in columns_to_look]
        columns_df.insert(0, table.index.name)

        df = pd.DataFrame(columns=columns_df)

        index = 0
        for i in table.index:
            row = [table[r][i] for r in columns_to_look]
            row.insert(0, i)
            df.loc[index] = row
            index = index + 1
        return(df)
    else:
        table = pd.DataFrame(table)
        table.reset_index(level=0, inplace=True)
        return table


def flatten_soft(dataframe):
    """Flattens a dataframe grouped on a multiple columns

    Parameters
    ----------
    table : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    res = pd.DataFrame()
    res[dataframe.index.name] = dataframe.index
    for col in dataframe.columns:
        name_tmp = ""
        for i in range(len(dataframe.columns[0])):
            name_tmp = name_tmp + '_'+str(col[i])
        res[str(name_tmp)] = dataframe[col].values
    return res


def convert_column_to_float(df: pd.DataFrame, col_name: str) -> None:
    """
    Converts a column to float inplace

    Parameters
    ----------
    df : pd.DataFrame
        _description_
    col_name : str
        _description_

    Returns
    -------
    None
    """
    # if values are string, have them with the right format
    try:
        df[col_name] = df[col_name].str.replace(',', '.')
    except Exception as e:
        # logging.warn(e)
        raise e
    df[col_name] = df[col_name].astype(float)


def format_column_to_dt(df: pd.DataFrame, col_name: str) -> None:
    df[col_name] = pd.to_datetime(df[col_name])