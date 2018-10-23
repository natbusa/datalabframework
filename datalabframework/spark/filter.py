import pandas as pd
import dateutil.parser as dp

from datetime import date, timedelta, datetime
import pyspark.sql.functions as F

def filter_by_date(obj, options):

    #Get ingest date
    now = datetime.now()
    today = datetime.combine(date.today(), datetime.min.time())

    end_date_str = options.get('end_date')
    start_date_str = options.get('start_date')
    window_str = options.get('window')

    #Get ingest key column
    column = options.get('column')

    # defaults
    end_date = dp.parse(end_date_str) if end_date_str else today
    start_date = dp.parse(start_date_str) if start_date_str else None
    window = pd.to_timedelta(window_str) if window_str else None

    # default calculated from window and end_date if both present
    if not start_date and window:
        start_date = end_date - window

    #build condition
    obj = obj.filter(F.to_timestamp(column) < end_date)
    obj = obj.filter(F.to_timestamp(column) >= start_date) if start_date else obj

    print('start date {}, end date {}'.format(start_date.isoformat(), end_date.isoformat()))
    print('filtered records', obj.count())

    return obj

def transform(obj, settings):

    #1. Get ingress policy
    policy = settings.get('policy')

    if policy=='date':
        obj = filter_by_date(obj, settings)

    return obj
