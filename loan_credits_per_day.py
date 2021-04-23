"""
Exploring the loan credits per users.
Version 3/24: It seems hard to find a meeting in the graphs without any sorting order or animations.
"""

import altair as alt
import numpy as np
import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform

def get_data(timeline):
 
  transactions = infra.pd.read_parquet("data/clean/transactions_TZ.parquet")

  # find the admin to exclude from data for both before and after covid
  df_admin = transactions[transactions["kind"] == "admin_topup"]
  
  # excluded retailers
  retailers = df_admin["dest_user"].unique()
  excluded_retailers = transactions[~transactions['user'].isin(retailers)]
  excluded_retailers = excluded_retailers[excluded_retailers['kind'] == 'user_transfer']

  #excluded self-transfer bug
  excluded_self = excluded_retailers[excluded_retailers['user'] != excluded_retailers['dest_user']]

  # normalize by time: taking temperal average divide by months
  # temperal average -- 238 days after (@11/24/2020) and before (@8/7/2019)
  start_date = "2019-08-07  00:00:00"
  lockdown_date = "2020-04-01  00:00:00"
  num_days = (pd.to_datetime(lockdown_date) - pd.to_datetime(start_date)).days
  end_date = str(pd.to_datetime(lockdown_date) + pd.DateOffset(num_days))
  assert(num_days == (pd.to_datetime(end_date) - pd.to_datetime(lockdown_date)).days)

  # Normalization: only get users present bf - after
  ## get only users from 238 days before
  query_start = f"(timestamp >= '{start_date}') & (timestamp < '{end_date}')"
  df_range = excluded_self.query(query_start)

  df_range = df_range[df_range['user'].notnull()]
  df_range = df_range[df_range['dest_user'].notnull()]

  all_users = pd.concat([df_range['user'], df_range['dest_user']]).rename('user').to_frame().drop_duplicates()
  all_users['key'] = 1
  x_users = pd.merge(all_users, all_users.rename(columns={'user': 'dest_user'}), on='key')[['user', 'dest_user']]

  # split the timeline before and after COVID
  ## March 25th, 2020 school closes  April 1st, 2020 roads to capital closed to town 
  query_before = f"(timestamp >= '{start_date}') & (timestamp < '{lockdown_date}')"
  # cut the last day
  query_after = f"(timestamp >= '{lockdown_date}') & (timestamp < '{end_date}')"

  df_before = df_range.query(query_before)
  df_after = df_range.query(query_after)

  if timeline == 'before':
    data_cleaned = df_range.query(query_before)
  elif timeline == 'after':
    data_cleaned = df_range.query(query_after)
  else:
    raise ValueError("Timeline should be only 'before' or 'after' the pandemic lockdown.")

  # TODO: right dataset to get?
  get_count(df_before, df_after, timeline)

  data_cleaned = data_cleaned.merge(x_users, on=['user', 'dest_user'], how='outer')
  data_cleaned['amount_idr'] = data_cleaned['amount_idr'].fillna(0)

  # total loan for each user
  total_loan = data_cleaned.groupby(["user", "dest_user"]).agg({"amount_idr" : 'sum'}) 
  total_loan['amount_idr'] = total_loan['amount_idr'].div(num_days)
  
  # # optional: exclude the maximum amount of loan (entire row)
  # total_loan = total_loan[total_loan['amount_idr'] != total_loan['amount_idr'].max()]

  # reset index for plotting
  df = pd.DataFrame(total_loan).reset_index()

  return df

def make_plot(df, timeline):

  count_origin = df['user'].nunique()
  count_dest =df['dest_user'].nunique()

  chrt = alt.Chart(
    df,
    title="Loaning Between User-User " + timeline + " Covid"
  ).mark_rect().encode(
    alt.X('user:N', 
      title= str(count_origin) +" Origin Users",
      sort=alt.EncodingSortField(field='dest_user', order='descending')
     ),
    alt.Y('dest_user:N',
     title= str(count_dest) + " Destination Users",
     sort=alt.EncodingSortField(field='dest_user', order='descending')
     ),
    alt.Color('amount_idr:Q', 
      title="Amount Loans",

      scale=alt.Scale(type='symlog', nice=True),
      condition={"test": "datum['amount_idr'] == 0", "value": "white"}
      ),
  ).save("./misc/charts/loan_credits_" + timeline +".html", scale_factor=2.0)

def get_count(df_before, def_after, timeline):

  if timeline == 'before': 
    total_loan = df_before['amount_idr'].sum(skipna= True)
    total_freq = len(df_before.index)
  elif timeline == 'after':
    total_loan = def_after['amount_idr'].sum(skipna= True)
    total_freq = len(def_after.index)
  else:
    raise ValueError("Timeline should be only 'before' or 'after' the pandemic lockdown.")

  print("Total loan all users for " + timeline + ": " + str(total_loan))
  print("Total frequency all users for " + timeline + ": " + str(total_freq))
  print(" ")
  print(" ")

  # loan_before = df_before.groupby(['user']).agg({"amount_idr" : 'sum'})
  # loan_before['time'] = 'before'

  # loan_after = def_after.groupby(['user']).agg({"amount_idr" : 'sum'})
  # loan_after['time'] = 'after'

  # loan = pd.concat([loan_before['user'], loan_after['user']]).to_frame()
  # print(loan.head())


  # alt.Chart(loan).mark_line().encode(
  #   x='user',
  #   y='amount_idr',
  #   color='time'
  # ).save("./misc/charts/amount.html", scale_factor=2.0)

  

  # return total_each_user

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)
    
    df_before = get_data("before")
    df_after = get_data("after")

    # make_plot(get_data("before"), "before")
    # make_plot(get_data("after"), "after")