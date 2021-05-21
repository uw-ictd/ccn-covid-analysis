"""
Exploring the loan credits per users.
Version 5/19: updated without self-transfer
"""

import altair as alt
import numpy as np
import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform

def get_data(timeline):
 
  # transactions = infra.pd.read_parquet("data/clean/transactions_TZ.parquet")
  # # df = pd.DataFrame(transactions).reset_index()
  # print(len(transactions.user))
  
  transactions = infra.pd.read_parquet('./data/derived/untainted_transactions_INDEX_timestamp.parquet')
  transactions["user"] = transactions["user"].astype(object)
  # print(transactions)

  # find the admin to exclude from data for both before and after covid
  df_admin = transactions[transactions["kind"] == "admin_topup"]
  
  # excluded retailers
  retailers = df_admin["dest_user"].unique()
  excluded_retailers = transactions[~transactions['user'].isin(retailers)]
  excluded_retailers = excluded_retailers[excluded_retailers['kind'] == 'user_transfer']

  # #excluded self-transfer bug
  excluded_self = excluded_retailers[excluded_retailers['user'] != excluded_retailers['dest_user']]

  # normalize by time: taking temperal average divide by months
  # temperal average -- 238 days after (@11/24/2020) and before (@8/7/2019) exactly 34 weeks.
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
  all_users['row_num'] = np.arange(len(all_users))
  all_users['user_derive'] = 'user-' + all_users['row_num'].astype(str).map(lambda x:x.zfill(2))
  x_users = pd.merge(all_users, all_users.rename(columns={'user': 'dest_user', 'user_derive': 'dest_user_derive'}), on='key')[['user', 'dest_user', 'user_derive', 'dest_user_derive']]

  derive_users = pd.merge(df_range, all_users, on='user')
  # derive_users.to_csv (r'/home/cwkt/Documents/ccn-traffic-analysis-2020/data/clean/' + 'full-nov-nb.csv', index = False, header=True)

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

  get_count(df_before, df_after, timeline)

  data_cleaned = df_range.merge(x_users, on=['user', 'dest_user'], how='outer')
  data_cleaned['amount_idr'] = data_cleaned['amount_idr'].fillna(0)

  # total loan for each user
  total_loan = data_cleaned.groupby(["user_derive", "dest_user_derive"]).agg({"amount_idr" : 'sum'}) 
  total_loan['amount_idr'] = total_loan['amount_idr'].div(num_days)
  
  # # optional: exclude the maximum amount of loan (entire row)
  # total_loan = total_loan[total_loan['amount_idr'] != total_loan['amount_idr'].max()]

  # reset index for plotting
  df = pd.DataFrame(total_loan).reset_index()

  # df.to_csv (r'/home/cwkt/Documents/ccn-traffic-analysis-2020/data/clean/' + timeline + '.csv', index = False, header=True)
  
  return df

def make_plot(df, timeline):

  count_origin = df['user_derive'].nunique()
  count_dest =df['dest_user_derive'].nunique()

  chrt = alt.Chart(
    df,
    # title="Loaning Between User-User " + timeline + " Covid"
    title={"text":"Total Loans Between User-User", "fontSize":22},
    width=2200,
    height=2200

  ).mark_rect().encode(
    alt.X('user_derive:N', 
      title= str(count_origin) +" Origin Users",
      sort=alt.EncodingSortField(field='dest_user_derive', order='descending')
     ),
    alt.Y('dest_user_derive:N',
     title= str(count_dest) + " Destination Users",
     sort=alt.EncodingSortField(field='dest_user_derive', order='descending')
     ),
    alt.Color('amount_idr:Q', 
      title="Amount Transferred (Rupiah)",
      legend={"titleFontSize": 18, "titleLimit": 400, "labelFontSize": 15},
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
    
    # df_before = get_data("before")
    # df_after = get_data("after")


    make_plot(get_data("before"), "before")
    make_plot(get_data("after"), "after")
    