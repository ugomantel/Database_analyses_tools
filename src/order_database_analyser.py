import os, sys

import pandas as pd

sys.path.append(os.getcwd() + '/src/')
from cohortes_analyses import build_cohort_df, create_cohorts_analysis_table
from features_creation import get_cohort
from utils import convert_column_to_float, format_column_to_dt


class OrderDatabaseAnalyser():
    def __init__(self):
        pass

    def load_db(self, path,
                id_customer,
                id_order,
                order_date,
                value_order,):
        self.raw_db = pd.read_csv(path)
        self.id_customer = id_customer
        self.id_order = id_order
        self.order_date = order_date
        self.value_order = value_order

    def preprocess(self):
        self.db = self.raw_db.copy()
        convert_column_to_float(self.db, self.value_order)
        format_column_to_dt(self.db, self.order_date)

    def create_cohort_feature(self, first_year=None):
        self.db = get_cohort(
            self.db, self.id_customer, self.id_order, self.order_date,
            first_year=first_year)

    def create_cohorts_df(self):
        self.cohorts_df = build_cohort_df(
            self.db,
            self.id_order,
            self.order_date,
            self.id_customer,
            'COHORT',
            self.value_order)

    def return_cohorts_analysis(self, analyse_type):
        return create_cohorts_analysis_table(self.cohorts_df, analyse_type)
