import datetime

# Define a conversion rate based on the current global market exchange rate.
IDR_TO_USD = 1.0 / 14150

# The max date is defined for the dataset and needs to be updated if the
# dataset changes.
MAX_DATE = datetime.datetime.strptime('2020-05-03 00:00:00', '%Y-%m-%d %H:%M:%S')

MIN_DATE = datetime.datetime.strptime('2019-03-10 00:00:00', '%Y-%m-%d %H:%M:%S')

# Notable events
OUTAGE_START = datetime.datetime.strptime('2019-07-30 00:00:00', '%Y-%m-%d %H:%M:%S')

OUTAGE_END = datetime.datetime.strptime('2019-09-01 00:00:00', '%Y-%m-%d %H:%M:%S')
