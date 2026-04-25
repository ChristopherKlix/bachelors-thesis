import datetime as dt
import pandas as pd


_show_warnings: bool = True
_default_tzinfo: str = 'Europe/Berlin'


def disable_warnings():
    global _show_warnings
    _show_warnings = False


class TimeRange:
    """A utility class for generating time ranges with timezone localization.

    This class provides static methods to get start and end datetimes for
    different time ranges (year, month, day, quarter) in a specified timezone.
    """

    def __init__(self):
        """This class should not be instantiated."""
        raise NotImplementedError('This class should not be instantiated.')

    ################################################################
    # Year
    ################################################################

    @staticmethod
    def get_year_range(
        year: int,
        tzinfo: str = _default_tzinfo
    ) -> tuple[dt.datetime, dt.datetime]:
        """Returns the start and end datetime for a given year.

        Args:
            year (int): The year for which to get the range.
            tzinfo (str): The timezone to apply.

        Returns:
            tuple[dt.datetime, dt.datetime]: Start and end of the year with timezone.
        """
        if year < 1900 or year > 3000:
            TimeRange._warning_year(year)

        start: dt.datetime = dt.datetime.fromisoformat(f'{year}-01-01 00:00')
        end: dt.datetime = dt.datetime.combine(dt.date(start.year + 1, 1, 1), dt.time.min)

        start = pd.Timestamp(start).tz_localize(tzinfo)
        end = pd.Timestamp(end).tz_localize(tzinfo)

        return start, end

    @staticmethod
    def get_current_year_range(
        tzinfo: str = _default_tzinfo
    ) -> tuple[dt.datetime, dt.datetime]:
        """Returns the start and end datetime for the current year.

        Args:
            tzinfo (str): The timezone to apply.

        Returns:
            tuple[dt.datetime, dt.datetime]: Start and end of the current year with timezone.
        """
        today: dt.date = dt.datetime.now().date()

        # Start of the current year (January 1st)
        start: dt.datetime = dt.datetime(today.year, 1, 1)

        end: dt.datetime = dt.datetime(today.year + 1, 1, 1)

        # Localize to the provided timezone
        start = pd.Timestamp(start).tz_localize(tzinfo)
        end = pd.Timestamp(end).tz_localize(tzinfo)

        return start, end

    @staticmethod
    def get_last_year_range(
        tzinfo: str = _default_tzinfo
    ) -> tuple[dt.datetime, dt.datetime]:
        """Returns the start and end datetime for the previous year.

        Args:
            tzinfo (str): The timezone to apply.

        Returns:
            tuple[dt.datetime, dt.datetime]: Start and end of the last year with timezone.
        """
        today: dt.date = dt.datetime.now().date()

        start: dt.datetime = dt.datetime.combine(today.replace(month=1, day=1) - dt.timedelta(days=1), dt.time.min)
        start = start.replace(day=1, month=1)

        end: dt.datetime = dt.datetime.combine(dt.date(start.year + 1, 1, 1), dt.time.min)

        start = pd.Timestamp(start).tz_localize(tzinfo)
        end = pd.Timestamp(end).tz_localize(tzinfo)

        return start, end

    ################################################################
    # Quarter
    ################################################################

    @staticmethod
    def get_quarter_range(
        year: int,
        quarter: int,
        tzinfo: str = _default_tzinfo
    ) -> tuple[dt.datetime, dt.datetime]:
        """Returns the start and end datetime for a specific quarter.

        Args:
            year (int): The year of the quarter.
            quarter (int): The quarter (1-4).
            tzinfo (str): The timezone to apply.

        Returns:
            tuple[dt.datetime, dt.datetime]: Start and end of the quarter with timezone.

        Raises:
            ValueError: If the provided value for quarter is not a valid quarter.
        """
        if year < 1900 or year > 3000:
            TimeRange._warning_year(year)

        if quarter < 1 or quarter > 4:
            raise ValueError(f'Invalid quarter: "{quarter}". Must be between 1 - 4.')

        month = (quarter - 1) * 3 + 1
        start = dt.datetime(year, month, 1)
        end = dt.datetime(year, month + 3, 1)

        start = pd.Timestamp(start).tz_localize(tzinfo)
        end = pd.Timestamp(end).tz_localize(tzinfo)

        return start, end

    @staticmethod
    def get_current_quarter_range(
        tzinfo: str = _default_tzinfo
    ) -> tuple[dt.datetime, dt.datetime]:
        """Returns the start and end datetime for the current quarter.

        Args:
            tzinfo (str): The timezone to apply.

        Returns:
            tuple[dt.datetime, dt.datetime]: Start and end of the current quarter with timezone.
        """
        # Determine the current quarter
        today: dt.date = dt.datetime.now().date()
        quarter_start_month = ((today.month - 1) // 3) * 3 + 1
        quarter_end_month = quarter_start_month + pd.DateOffset(months=3)

        # Construct datetime objects
        start: dt.datetime = dt.datetime(today.year, quarter_start_month, 1)
        end: dt.datetime = dt.datetime(today.year, quarter_end_month, 1)

        # Localize to the provided timezone
        start = pd.Timestamp(start).tz_localize(tzinfo)
        end = pd.Timestamp(end).tz_localize(tzinfo)

        return start, end

    ################################################################
    # Month
    ################################################################

    @staticmethod
    def get_month_range(
        year: int,
        month: int,
        tzinfo: str = _default_tzinfo
    ) -> tuple[dt.datetime, dt.datetime]:
        """Returns the start and end datetime for a given month.

        Args:
            year (int): The year of the month.
            month (int): The month (1-12).
            tzinfo (str): The timezone to apply.

        Returns:
            tuple[dt.datetime, dt.datetime]: Start and end of the month with timezone.

        Raises:
            ValueError: If the provided value for month is not a valid month.
        """
        if year < 1900 or year > 3000:
            TimeRange._warning_year(year)

        if month < 1 or month > 12:
            raise ValueError(f'Invalid month: "{month}". Must be between 1 - 12.')

        start = dt.datetime.fromisoformat(f'{year}-{month:02d}-01 00:00')
        end = start + pd.DateOffset(months=1)

        start = pd.Timestamp(start).tz_localize(tzinfo)
        end = pd.Timestamp(end).tz_localize(tzinfo)

        return start, end

    @staticmethod
    def get_current_month_range(
        tzinfo: str = _default_tzinfo
    ) -> tuple[dt.datetime, dt.datetime]:
        """Returns the start and end datetime for the current month.

        Args:
            tzinfo (str): The timezone to apply.

        Returns:
            tuple[dt.datetime, dt.datetime]: Start and end of the month with timezone.
        """
        today: dt.date = dt.datetime.now().date()
        start = dt.datetime.fromisoformat(f'{today.year}-{today.month:02d}-01 00:00')
        end = start + pd.DateOffset(months=1)

        start = pd.Timestamp(start).tz_localize(tzinfo)
        end = pd.Timestamp(end).tz_localize(tzinfo)

        return start, end

    ################################################################
    # Day
    ################################################################

    @staticmethod
    def get_day_range(
        year: int,
        month: int,
        day: int,
        tzinfo: str = _default_tzinfo
    ) -> tuple[dt.datetime, dt.datetime]:
        """Returns the start and end datetime for a specific day.

        Args:
            year (int): The year of the day.
            month (int): The month of the day.
            day (int): The day of the month.
            tzinfo (str): The timezone to apply.

        Returns:
            tuple[dt.datetime, dt.datetime]: Start and end of the day with timezone.

        Raises:
            ValueError: If the provided year, month, and day do not form a valid date.
        """
        if year < 1900 or year > 3000:
            TimeRange._warning_year(year)

        try:
            start = dt.datetime(year, month, day)
        except ValueError:
            raise ValueError(f'Invalid date: {year}-{month:02d}-{day:02d}')

        end = start + dt.timedelta(days=1)

        start = pd.Timestamp(start).tz_localize(tzinfo)
        end = pd.Timestamp(end).tz_localize(tzinfo)

        return start, end

    ################################################################
    # Helper Methods
    ################################################################

    @staticmethod
    def _warning_year(year: int):
        """Prints a warning message if the year is potentially problematic, based on the `show_warnings` flag.

        Args:
            year (int): The year to check.

        Returns:
            None: Prints a warning if the year is problematic and warnings are enabled.
        """
        if not _show_warnings:
            return

        print(f'Warning. The value for year "{year}" might be out of the expected range.')
