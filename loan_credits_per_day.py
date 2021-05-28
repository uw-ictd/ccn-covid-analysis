"""
Exploring the loan credits per users.
Version 5/28: Normalization only get users present bf and after
"""

import altair as alt
import numpy as np
import pandas as pd
 
import infra.constants
import infra.dask
import infra.pd
import infra.platform

def get_data(timeline):
  
  transactions = infra.pd.read_parquet('./data/derived/untainted_transactions_INDEX_timestamp.parquet').reset_index()
  transactions["user"] = transactions["user"].astype(object)
  print("Total number of transactions: " + str(len(transactions.user)))

  # find the admin to exclude from data for both before and after covid
  df_admin = transactions[transactions["kind"] == "admin_topup"]
  
  # excluded retailers
  retailers = df_admin["dest_user"].unique()
  excluded_retailers = transactions[~transactions['user'].isin(retailers)]
  excluded_retailers = excluded_retailers[excluded_retailers['kind'] == 'user_transfer']

  # #excluded self-transfer bug
  excluded_self = excluded_retailers[excluded_retailers['user'] != excluded_retailers['dest_user']]

  
  # exclude dirty money
  start_tainted = "2020-05-24"
  end_tainted = "2020-06-14"

  query_start = f"(timestamp < '{start_tainted}') | (timestamp > '{end_tainted}')"
  df_no_tainted = excluded_self.query(query_start)

  # normalize by time: taking temperal average divide by months
  # temperal average -- 238 days after (@12/16/2020) and before (@8/7/2019) exactly 34 weeks ---excluding tainted date.
  start_date = "2019-08-07  00:00:00"
  lockdown_date = "2020-04-01  00:00:00"

  num_days = (pd.to_datetime(lockdown_date) - pd.to_datetime(start_date)).days
  tainted_date_makeup = pd.DateOffset(7 * 3)
  end_date = str(pd.to_datetime(lockdown_date) + pd.DateOffset(num_days) + tainted_date_makeup)

  # assert(num_days == (pd.to_datetime(end_date) - pd.to_datetime(lockdown_date)).days)

  # get only users from 238 days before
  query_start = f"(timestamp >= '{start_date}') & (timestamp < '{end_date}')"
  df_range = df_no_tainted.query(query_start)


  df_range = df_range[df_range['user'].notnull()]
  df_range = df_range[df_range['dest_user'].notnull()]

  def get_users(df):
    return pd.concat([df['user'], df['dest_user']]).rename('user').to_frame().drop_duplicates()
  
  all_users = get_users(df_range)
  all_users['key'] = 1
  all_users['row_num'] = np.arange(len(all_users))
  all_users['user_derive'] = 'user-' + all_users['row_num'].astype(str).map(lambda x:x.zfill(2))
  x_users = pd.merge(all_users, all_users.rename(columns={'user': 'dest_user', 'user_derive': 'dest_user_derive'}), on='key')[['user', 'dest_user', 'user_derive', 'dest_user_derive']]

  df_range = pd.merge(df_range, all_users, on='user')
  # df_range.to_csv (r'/home/cwkt/Documents/ccn-traffic-analysis-2020/data/clean/' + 'full-nov-nb.csv', index = False, header=True)

  # split the timeline before and after COVID
  ## March 25th, 2020 school closes  April 1st, 2020 roads to capital closed to town 
  query_before = f"(timestamp >= '{start_date}') & (timestamp < '{lockdown_date}')"
  # cut the last day
  query_after = f"(timestamp >= '{lockdown_date}') & (timestamp < '{end_date}')"

  df_before = df_range.query(query_before)
  df_after = df_range.query(query_after)

  
  # Normalization:only get users present bf and after
  #    do inner join
  all_before_users = get_users(df_before)
  all_after_users = get_users(df_after)
  all_after_users_user = all_after_users['user'].unique()
  intersection = all_before_users[all_before_users['user'].isin(all_after_users_user)]['user'].unique()

  #   both users and dest_users must be in the set of before and after dataset
  intersect_before = df_before[df_before['user'].isin(intersection) & df_before['dest_user'].isin(intersection)]
  intersect_after = df_after[df_after['user'].isin(intersection) & df_after['dest_user'].isin(intersection)]

  #   check if any of the users or dest_user never exists before/after
  # print(~df_before['dest_user'].isin(intersection))
  # print(~df_after['dest_user'].isin(intersection))

  if timeline == 'before':
    data_cleaned = intersect_before
  elif timeline == 'after':
    data_cleaned = intersect_after
  else:
    raise ValueError("Timeline should be only 'before' or 'after' the pandemic lockdown.")

  get_count(df_before, df_after, timeline)

  data_cleaned = df_range.merge(x_users, on=['user', 'dest_user'], how='outer')
  data_cleaned['user_derive'] = data_cleaned['user_derive_x']
  data_cleaned['amount_idr'] = data_cleaned['amount_idr'].fillna(0)

  # total loan for each user
  total_loan = data_cleaned.groupby(["user_derive", "dest_user_derive"]).agg({"amount_idr" : 'sum'}) 
  total_loan['amount_idr'] = total_loan['amount_idr'].div(num_days)

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

  
  loan_week = total_loan / 34.00
  loan_day = total_loan / 238.00
  freq_week = total_freq / 34
  freq_day = total_freq / 238


  print("Total loan all users for " + timeline + ": " + str(total_loan))
  print(" Average loan per week for " + timeline + ": " + str(loan_week))
  print(" Average loan per day for " + timeline + ": " + str(loan_day))

  print("Total frequency all users for " + timeline + ": " + str(total_freq))
  print(" Average frequency per week for " + timeline + ": " + str(freq_week))
  print(" Average frequency per day for " + timeline + ": " + str(freq_day))

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
    