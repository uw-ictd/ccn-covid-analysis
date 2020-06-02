#!/usr/bin/python

import pandas as pd
import numpy as np

BTS_COST = 6600
ANTENNA_COST = 516
SIM_COST = 650
MONTHLY_VSAT = 800
MONTHS = 13

#13 months
def generate_install_cost():
    dates = pd.date_range('20190311', periods=MONTHS, freq='M')
    costs = [BTS_COST + ANTENNA_COST + SIM_COST + MONTHLY_VSAT]
    for i in range(MONTHS-1):
        costs.append(costs[i] + MONTHLY_VSAT)
        
    df = pd.DataFrame(costs, index=dates, columns = (['cost']))
    return df

print(generate_install_cost())
