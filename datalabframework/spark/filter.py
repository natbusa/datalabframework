import pandas as pd
import dateutil.parser as dp

from datetime import date, datetime
import pyspark.sql.functions as F

from .. import logging


def filter_by_date(obj, options):
    logger = logging.getLogger()

    # Get ingest date
    today = datetime.combine(date.today(), datetime.min.time())

    end_date_str = options.get('end_date')
    start_date_str = options.get('start_date')
    window_str = options.get('window')
    tzone = options.get('tzone', 'GMT')

    # Get ingest key column
    column = options.get('column')

    # defaults
    end_date = dp.isoparse(end_date_str) if end_date_str else today
    start_date = dp.isoparse(start_date_str) if start_date_str else None
    window = pd.to_timedelta(window_str) if window_str else None

    # default calculated from window and end_date if both present
    if not start_date and window:
        start_date = end_date - window

    # build condition
    if '_date' in obj.columns:
        obj = obj.filter(F.to_timestamp('_date') < end_date)
        obj = obj.filter(F.to_timestamp('_date') >= start_date) if start_date else obj

    obj = obj.filter(F.to_utc_timestamp(F.to_timestamp(column), tzone) < end_date)
    obj = obj.filter(F.to_utc_timestamp(F.to_timestamp(column), tzone) >= start_date) if start_date else obj

    # print('start date {}, end date {}'.format(start_date.isoformat(), end_date.isoformat()))
    if start_date:
        logger.info('start date {}, end date {}'.format(start_date.isoformat(), end_date.isoformat()),
                    extra={'dlf_type': 'engine.read'})
    else:
        logger.info('start date: none, end date {}'.format(end_date.isoformat()), extra={'dlf_type': 'engine.read'})

    logger.info('filtered records {}'.format(obj.count()), extra={'dlf_type': 'engine.read'})

    return obj


def transform(obj, settings):
    # 1. Get ingress policy
    policy = settings.get('policy')

    if policy == 'date':
        obj = filter_by_date(obj, settings)

    return obj
