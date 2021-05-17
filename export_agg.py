"""
Exploring the loan credits per users.
Version 3/24: It seems hard to find a meeting in the graphs without any sorting order or animations.
"""


import infra.constants
import infra.dask
import infra.pd
import infra.platform

def export_data(file_name, path):
 
  df = infra.pd.read_parquet(path)
  print(df.head())

  df.to_csv(r'/home/cwkt/Documents/ccn-traffic-analysis-2020/data/aggregates/' + file_name + '.csv')

if __name__ == "__main__":
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.width', None)
    # pd.set_option('display.max_rows', None)

    export_data('bytes_per_category_per_user_per_day','./data/aggregates/bytes_per_category_per_user_per_day.parquet')
    export_data('bytes_per_org_per_user_per_day','./data/aggregates/bytes_per_org_per_user_per_day.parquet')