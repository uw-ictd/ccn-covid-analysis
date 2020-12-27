import datetime

# The amount of time to shift to align UTC with local time
LOCAL_TIME_UTC_OFFSET_HOURS = 9

# Define a conversion rate based on the current global market exchange rate.
IDR_TO_USD = 1.0 / 14150

MIN_DATE = datetime.datetime.strptime('2019-03-10 00:00:00', '%Y-%m-%d %H:%M:%S')

# The max date is defined for the dataset and needs to be updated if the
# dataset changes.
# MAX_DATE = datetime.datetime.strptime('2019-05-03 00:00:00', '%Y-%m-%d %H:%M:%S')
MAX_DATE = MIN_DATE + datetime.timedelta(weeks=88)

# Notable events
OUTAGE_START = datetime.datetime.strptime('2019-07-30 00:00:00', '%Y-%m-%d %H:%M:%S')

OUTAGE_END = datetime.datetime.strptime('2019-09-01 00:00:00', '%Y-%m-%d %H:%M:%S')

# Anonymity
MIN_K_ANON = 5
