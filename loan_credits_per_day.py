"""
Exploring the loan credits per users.
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

  # normalize by time: taking temperal average divide by months
  # temperal average -- 238 days after (11/24/2020) and before (8/7/2019)
  start_date = pd.to_datetime("2019-08-07  00:00:00")
  lockdown_date = pd.to_datetime("2020-04-01  00:00:00")

  num_days = (lockdown_date - start_date).days
  query_start = "(timestamp >= '2019-08-07  00:00:00')"
  
  # Normalization: only get users present bf - after
  ## get only users from 238 days before
  query_before = "(timestamp >= '2019-08-07  00:00:00') & (timestamp < '2020-04-01 00:00:00')"
  df_range = excluded_retailers.query(query_start)

  users_before = []
  users_before = df_range.query(query_before)['user'].unique()
  users_before = users_before[users_before != None]

  dest_users = df_range.query(query_before)['dest_user'].unique()
  users_before = np.append(users_before, dest_users[dest_users != None])
  users_before = users_before.unique()

  data_cleaned = []
  
  # split the timeline before and after COVID
  ## March 25th, 2020 school closes  April 1st, 2020 roads to capital closed to town 
  if timeline == 'before':
    # 238 day before (8/7/2019)
    data_cleaned =  df_range.query(query_before)
    print("num users before" + str(data_cleaned['user'].nunique())) 

  elif timeline == 'after':
    df_after =  df_range.query("timestamp >= '2020-04-01 00:00:00' ")

     #  the dest users be from the same datatset too
    df = df_after[df_after['user'].isin(users_before)]                              
    df = df_after[df_after['dest_user'].isin(users_before)]  

    df = df.merge(users_before, how = 'outer', on=['user', 'dest_user'])
    data_cleaned = df['amount_idr'].fillna(0)

    print("num users after" + str(data_cleaned['user'].nunique())) 

  else:
    print("Timeline should be only 'before' or 'after' the pandemic lockdown.")
    # TODO raise error
  

  # total loan for each user
  total_loan = data_cleaned.groupby(["user", "dest_user"]).agg({"amount_idr" : 'sum'}) 
  total_loan = total_loan['amount_idr'].div(num_days)
  
  # optional: exclude the maximum amount of loan (entire row)
  total_loan = total_loan[total_loan['amount_idr'] != total_loan['amount_idr'].max()]

  # drop the rows of retailers
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
      # TODO making axis more static - build all the empty users to fill in 0 

     ),
    alt.Y('dest_user:N',
     title= str(count_dest) + " Destination Users",
     sort=alt.EncodingSortField(field='dest_user', order='descending')
      # TODO making axis more static - build all the empty users to fill in 0 
     ),
    alt.Color('amount_idr:Q', 
      title="Amount Loans",

      #TODO should I apply log scale? Do in panda handle log0 = 0
      scale=alt.Scale(type='linear', nice=True),          #  TODO cheange  scale in domain 
      ),
  ).save("./misc/charts/loan_credits_" + timeline +".html", scale_factor=2.0)


if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)
    
    get_data("before")
    
    # get_data("after")

    # make_plot(get_data("before"), "before")
    # make_plot(get_data("after"), "after")