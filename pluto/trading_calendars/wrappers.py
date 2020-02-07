from trading_calendars import TradingCalendar

class OpenOffsetFix(TradingCalendar):
    def __init__(self, start, end, calendar_type):
        '''
        Parameters
        ----------
        calendar_type
        '''
        self._calendar_type = calendar_type
        self._open_offset = calendar_type.open_offset.fget(self)

        try:
            self.regular_early_close = calendar_type.regular_early_close
        except AttributeError:
            pass

        super(OpenOffsetFix, self).__init__(start, end)

    @property
    def close_offset(self):
        if self._open_offset == -1:
            return 1
        else:
            return 0

    @property
    def name(self):
        try:
            return self._calendar_type.name.fget(self)
        except AttributeError:
            return self._calendar_type.name

    @property
    def close_times(self):
        try:
            return self._calendar_type.close_times.get(self)
        except AttributeError:
            return self._calendar_type.close_times

    @property
    def open_times(self):
        try:
            return self._calendar_type.open_times.fget(self)
        except AttributeError:
            return self._calendar_type.open_times

    @property
    def tz(self):
        try:
            return self._calendar_type.tz.fget(self)
        except AttributeError:
            return self._calendar_type.tz

    @property
    def special_closes_adhoc(self):
        return self._calendar_type.special_closes_adhoc.fget(self)

    @property
    def special_closes(self):
        return self._calendar_type.special_closes.fget(self)

    @property
    def special_opens_adhoc(self):
        return self._calendar_type.special_opens_adhoc.fget(self)

    @property
    def special_opens(self):
        return self._calendar_type.special_opens.fget(self)

    @property
    def adhoc_holidays(self):
        return self._calendar_type.adhoc_holidays.fget(self)

    @property
    def regular_holidays(self):
        return self._calendar_type.regular_holidays.fget(self)

    def execution_time_from_open(self, open_dates):
        return self._calendar_type.execution_time_from_open(self, open_dates)

    def execution_time_from_close(self, close_dates):
        return self._calendar_type.execution_time_from_close(self, close_dates)