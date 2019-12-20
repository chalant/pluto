class QuoteParameters(object):
    __slots__ = ['ticker']

    def __init__(self, ticker):
        self.ticker = ticker

class HistoryParameters(object):
    __slots__ = ['ticker', 'start', 'end']

    def __init__(self, ticker, start, end):
        self.ticker = ticker
        self.start = start
        self.end = end

class EarningsCalendar(object):
    __slots__ = ['date']

    def __init__(self, date):
        self.date = date