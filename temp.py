import pandas as pd

import infra.constants
import infra.dask
import infra.pd
import infra.platform



transactions = infra.pd.read_parquet('./data/derived/untainted_transactions_INDEX_timestamp.parquet')
transactions["user"] = transactions["user"].astype(object)

org = infra.pd.read_parquet('./data/aggregates/bytes_per_org_per_user_per_day.parquet')
print(org.head())