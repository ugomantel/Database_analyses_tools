# %%

%load_ext autoreload
%autoreload 2

import os

import pandas as pd

# from src.features_creation import get_cohort
# from src.cohortes_analyses import build_cohort
from src.order_database_analyser import OrderDatabaseAnalyser

# %%
# Import example data

EXAMPLE_CSV_PATH = os.getcwd() + '/input_data.csv'

analyser = OrderDatabaseAnalyser()
analyser.load_db(EXAMPLE_CSV_PATH, 'id_customer', 'id_order', 'order_date', 'unit_price_TTC')
analyser.preprocess()

# raw_df = pd.read_csv(EXAMPLE_CSV_PATH, sep=',')
# raw_df.head()

# %%

# TODO: ensure that created columns names are not already present
# TODO: check that all dates formats can be run

# Add cohort feature to 
# TODO: rename
# updated_df = get_cohort(raw_df, 'id_customer', 'id_order', 'order_date')
analyser.create_cohort_feature()

# %%

# Create cohorts df
# TODO: rename
# cohorts_df = build_cohort(analyser.db, 'id_order', 'order_date', 'id_customer', 
#                           id_cohort='COHORT', value_order='unit_price_TTC')
analyser.create_cohorts_df()

# %%
analyser.return_cohorts_analysis('sales')
# %%
analyser.return_cohorts_analysis('activity')
# %%
analyser.return_cohorts_analysis('test')

# %%

from bridge_analysis import build_waterfall


waterfall_df = build_waterfall(
                               analyser.db,
                               'id_customer',
                               'order_date',
                               'unit_price_TTC',
                               'COHORT',)

waterfall_table = waterfall_df.sum()

# %%

# %%

df_by_cohort = flatten(df.groupby(['COHORT']).agg({'SALES_2017': sum, 'SALES_2018': sum,'SALES_2019': sum, 'SALES_2020': sum,
'SALES_2021': sum,
'ACTIVE_2017': sum, 'ACTIVE_2018': sum,'ACTIVE_2019': sum, 'ACTIVE_2020': sum,
'ACTIVE_2021': sum, }))
df_by_cohort.columns = ['COHORT','SALES_2017', 'SALES_2018', 'SALES_2019','SALES_2020', 'SALES_2021',
'ACTIVE_2017', 'ACTIVE_2018', 'ACTIVE_2019','ACTIVE_2020', 'ACTIVE_2021']

