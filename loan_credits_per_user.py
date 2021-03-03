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

def explore_table():
 
  transactions = infra.pd.read_parquet("data/clean/transactions_TZ.parquet")

  # find the admin to exclude from data for both before and after covid
  df_admin = transactions[transactions["kind"] == "admin_topup"]
  
  # excluded retailers
  retailers = df_admin["dest_user"].unique()
  df = transactions[~transactions['user'].isin(retailers)]

  # split the timeline before and after COVID
  ## March 25th, 2020 school closes  April 1st, 2020 roads to capital closed to town 
  transactions_before =  df.query("timestamp < '2020-04-01 00:00:00' ")
  transactions_after =  df.query("timestamp >= '2020-04-01 00:00:00' ")

  users = df.groupby(["user", "dest_user"]).agg({"amount_idr" : 'sum'})

  # drop the rows of retailers
  data_cleaned = pd.DataFrame(users).reset_index()



  
  

  # exclude the maximum amount of loan (entire row)
  # data_cleaned = data_cleaned[data_cleaned['amount_idr'] != data['amount_idr'].max()]
 


  chrt = alt.Chart(
    data_cleaned,
    title="Loaning money from user to user"
  ).mark_rect().encode(
    x=alt.X('user:N', title="Origin Users"),
    y=alt.Y('dest_user:N', title="Destination Users"),
    color=alt.Color('amount_idr:Q', title="Amount Loans"),\
  ).save("./misc/charts/loan_credits_per_user.html", scale_factor=2.0)



# def make_plot(df):
#   # to be refactor


if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)
    
    explore_table()
    # make_plot()