"""
Exporting and cleaning the agg data

Q: 
- exclude retailers/admin?

"""


# from os import defpath
# import altair as alt
# import numpy as np
import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform

def export_data(file_name, path, timeline):
 
  df = infra.pd.read_parquet(path)
  df = pd.DataFrame(df).reset_index()

  transactions = infra.pd.read_parquet("./data/derived/untainted_transactions_INDEX_timestamp.parquet")
  transactions["user"] = transactions["user"].astype(object)

  # find the admin to exclude from data for both before and after covid
  df_admin = transactions[transactions["kind"] == "admin_topup"]
  
  # excluded retailers
  retailers = df_admin["dest_user"].unique()
  excluded_retailers = df[~df['user'].isin(retailers)]


  # normalize by time: taking temperal average divide by months
  # temperal average -- 238 days after (@11/24/2020) and before (@8/7/2019) exactly 34 weeks.
  start_date = "2019-08-07"
  lockdown_date = "2020-04-01"
  num_days = (pd.to_datetime(lockdown_date) - pd.to_datetime(start_date)).days
  end_date = str(pd.to_datetime(lockdown_date) + pd.DateOffset(num_days))
  assert(num_days == (pd.to_datetime(end_date) - pd.to_datetime(lockdown_date)).days)


  # Normalization: only get users present bf - after
  ## get only users from 238 days before
  query_start = f"(day >= '{start_date}') & (day < '{end_date}')"
  df_range = excluded_retailers.query(query_start)
  df_range = df_range[df_range['user'].notnull()]


  # exclude dirty money
  start_tainted = "2020-05-24"
  end_tainted = "2020-06-11"

  query_start = f"(day < '{start_tainted}') | (day > '{end_tainted}')"
  df_no_tainted = df_range.query(query_start)



  # split the timeline before and after COVID
  ## March 25th, 2020 school closes  April 1st, 2020 roads to capital closed to town 
  query_before = f"(day >= '{start_date}') & (day < '{lockdown_date}')"
  # cut the last day
  query_after = f"(day >= '{lockdown_date}') & (day < '{end_date}')"

  if timeline == 'before':
    df = df_no_tainted.query(query_before)
    data_cleaned = give_week_num(df)

  elif timeline == 'after':
    df = df_no_tainted.query(query_after)
    data_cleaned = give_week_num(df)
  else:
    raise ValueError("Timeline should be only 'before' or 'after' the pandemic lockdown.")

  # print("------------" + timeline)
  # print(data_cleaned[data_cleaned.isna().any(axis=1)])

  data_cleaned.to_csv(r'/home/cwkt/Documents/ccn-traffic-analysis-2020/data/aggregates/' + file_name + '_'+  timeline + '.csv', index = False, header=True)


def give_week_num(df):
  
  # no need to shift 7 days
  # df['start_week'] = pd.to_datetime(df['day']) - pd.to_timedelta(7, unit='d')

  df = df.groupby(['org', pd.Grouper(key='day', freq='W-WED')])['bytes_down'].mean().reset_index().sort_values('day')
  return df


if __name__ == "__main__":
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.width', None)
    # pd.set_option('display.max_rows', None)

    # export_data('bytes_per_category_per_user_per_day',
    #   './data/aggregates/bytes_per_category_per_user_per_day.parquet', 
    #   'before')

    # export_data('bytes_per_category_per_user_per_day',
    #   './data/aggregates/bytes_per_category_per_user_per_day.parquet', 
    #   'after')

    # export_data('bytes_per_org_per_user_per_day',
    #   './data/aggregates/bytes_per_org_per_user_per_day.parquet',
    #   'before')

    # export_data('bytes_per_org_per_user_per_day',
    #   './data/aggregates/bytes_per_org_per_user_per_day.parquet',
    #   'after')


    # org data

    export_data('org_before',
    './data/aggregates/bytes_per_org_per_user_per_day.parquet',
    'before')

    export_data('org_after',
    './data/aggregates/bytes_per_org_per_user_per_day.parquet',
    'after')