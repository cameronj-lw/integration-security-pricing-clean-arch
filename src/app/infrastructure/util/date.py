"""
Date related utils
"""
from datetime import date, timedelta
import calendar


from app.infrastructure.sql_tables import LWDBCalendarTable


def generate_eom_dates(start_date, end_date=None, include_end_date=True):
    """
    Generate end of month business days from start_date to end_date inclusive. end_date will
    default to last business day if not set. Optionally also include end_date, making
    sure not to include it twice.

    Args:
    - start_date (datetime.date): The start date or datetime
    - end_date (datetime.date): Optional end date or datetime. Will default to last business day if not set
    - include_end_date (bool): Flag to indicate we should include the end date in the series
                             regardless of whether it's an end of month date
    
    Returns: Pandas Series of datetimes
    """
    import pandas as pd
    if end_date is None:
        end_date = date.today() - pd.tseries.offsets.BDay()

    # 'BM' is Pandas shorthand for business month end
    dates = pd.date_range(start_date, end_date, freq='BM')
    # Convert from DatetimeIndex to Series
    dates = pd.Series(dates)

    if include_end_date and not (dates == end_date).any():
        # Convert to series and append
        dates = dates.append(pd.Series([end_date], dtype='datetime64[ns]'))
        dates = dates.reset_index(drop=True)

    return dates


def get_nearest_month_end(ref_date):
    """
    Get the closest past month end date to reference date

    Args:
    - ref_date (datetime.date): Reference date

    Returns: 
    - datetime.date: Closest ME date
    """
    # Create a list of all possible nearest MEs for the ref date.
    me_dates = [
        date(ref_date.year-1, 12, 31),
        date(ref_date.year, 1, 31),
        date(ref_date.year, 2, 28),
        date(ref_date.year, 3, 31),
        date(ref_date.year, 4, 30),
        date(ref_date.year, 5, 31),
        date(ref_date.year, 6, 30),
        date(ref_date.year, 7, 31),
        date(ref_date.year, 8, 31),
        date(ref_date.year, 9, 30),
        date(ref_date.year, 10, 31),
        date(ref_date.year, 11, 30),
        date(ref_date.year, 12, 31)
    ]
    if calendar.isleap(ref_date.year):
        me_dates = [
            date(ref_date.year-1, 12, 31),
            date(ref_date.year, 1, 31),
            date(ref_date.year, 2, 29),
            date(ref_date.year, 3, 31),
            date(ref_date.year, 4, 30),
            date(ref_date.year, 5, 31),
            date(ref_date.year, 6, 30),
            date(ref_date.year, 7, 31),
            date(ref_date.year, 8, 31),
            date(ref_date.year, 9, 30),
            date(ref_date.year, 10, 31),
            date(ref_date.year, 11, 30),
            date(ref_date.year, 12, 31)
        ]

    min_delta = timedelta.max
    result = None

    # Go through the list and find the closest past quarter end date.
    # TODO: Take advantage of the fact that the list above is sorted
    for me_date in me_dates:
        delta = ref_date - me_date
        if delta.days >= 0 and delta < min_delta:
            min_delta = delta
            result = me_date

    return result


def get_nearest_quarter_end(ref_date):
    """
    Get the closest past quarter end date to reference date

    Args:
    - ref_date (datetime.date): Reference date
    
    Returns:
    - datetime.date: Closest QE date
    """
    # Create a list of all possible nearest QEs for the ref date.
    qe_dates = [
        date(ref_date.year-1, 12, 31),
        date(ref_date.year, 3, 31),
        date(ref_date.year, 6, 30),
        date(ref_date.year, 9, 30),
        date(ref_date.year, 12, 31)
    ]

    min_delta = timedelta.max
    result = None

    # Go through the list and find the closest past quarter end date.
    # TODO: Take advantage of the fact that the list above is sorted
    for qe_date in qe_dates:
        delta = ref_date - qe_date
        if delta.days >= 0 and delta < min_delta:
            min_delta = delta
            result = qe_date

    return result


def get_nearest_year_end(ref_date):
    """
    Get the closest past year end date to reference date

    Args:
    - ref_date (datetime.date): Reference date
    
    Returns:
    - datetime.date: Closest YE date
    """
    # Create a list of all possible nearest YEs for the ref date.
    ye_dates = [
        date(ref_date.year-1, 12, 31),
        date(ref_date.year, 12, 31)
    ]

    min_delta = timedelta.max
    result = None

    # Go through the list and find the closest past year end date.
    # TODO: Take advantage of the fact that the list above is sorted
    for ye_date in ye_dates:
        delta = ref_date - ye_date
        if delta.days >= 0 and delta < min_delta:
            min_delta = delta
            result = ye_date

    return result


def get_current_bday(ref_date):
    """
    Get the closest past or current business date to reference date

    Args:
    - ref_date (datetime.date): Reference date
    
    Returns:
    - datetime.date: Closest biz date
    """
    calendar = LWDBCalendarTable().read_for_date(ref_date)
    curr_bday = calendar['curr_bday']
    return curr_bday[0].date()


def get_previous_bday(ref_date):
    """
    Get the previous business date to reference date

    Args:
    - ref_date (datetime.date): Reference date
    
    Returns:
    - datetime.date: Previous biz date
    """
    calendar = LWDBCalendarTable().read_for_date(ref_date)
    prev_bday = calendar['prev_bday']
    return prev_bday[0].date()


def get_next_bday(ref_date):
    """
    Get the next business date to reference date

    Args:
    - ref_date (datetime.date): Reference date
    
    Returns:
    - datetime.date: Next biz date
    """
    calendar = LWDBCalendarTable().read_for_date(ref_date)
    prev_bday = calendar['next_bday']
    return prev_bday[0].date()


def format_time(t):
    """
    Get time string with only 3 decimal places for seconds

    Args:
    - t (datetime.time): Time to format
    
    Returns:
    - String representing time with seconds chopped off after 3 decimal places
    """
    s = t.strftime('%Y-%m-%d %H:%M:%S.%f')
    return s[:-3]
