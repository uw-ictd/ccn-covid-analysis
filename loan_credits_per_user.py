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
  data_cleaned = transactions[~transactions['user'].isin(retailers)]

  # TODO only get users present bf - after

  # TODO normalize by time 1) 7month before/after  2) taking temperal average divide by months

  # split the timeline before and after COVID
  ## March 25th, 2020 school closes  April 1st, 2020 roads to capital closed to town 
  if timeline == 'before':
    data_cleaned =  data_cleaned.query("timestamp < '2020-04-01 00:00:00' ")
  elif timeline == 'after':
    data_cleaned =  data_cleaned.query("timestamp >= '2020-04-01 00:00:00' ")
  else:
    print("Timeline should be only 'before' or 'after' the pandemic lockdown.")
    return NULL

  # total loan for each user
  total_loan = data_cleaned.groupby(["user", "dest_user"]).agg({"amount_idr" : 'sum'})
  
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
    
    make_plot(get_data("before"), "before")
    make_plot(get_data("after"), "after")