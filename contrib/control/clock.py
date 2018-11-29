from enum import IntEnum
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, date

from trading_calendars.utils.pandas_utils import days_at_time

'''module for events extension...'''

from zipline.gens import sim_engine as se


class Events(IntEnum):
	BAR = se.BAR
	SESSION_START = se.SESSION_START
	SESSION_END = se.SESSION_END
	MINUTE_END = se.MINUTE_END
	BEFORE_TRADING_START_BAR = se.BEFORE_TRADING_START_BAR
	FINISH = se.FINISH

BAR,SESSION_START,SESSION_END,MINUTE_END,BEFORE_TRADING_START_BAR,FINISH = list(Events)

class Clock(ABC):
	@abstractmethod
	def __iter__(self):
		raise NotImplementedError


class MinuteClockWrapper(Clock):
	'''emits an additional event to signal that the clock has finished executing'''

	def __init__(self, last_session, clock):
		self._clock = clock
		self._last_session = last_session
		self._itr = None
		self._prev_dt = None
		self._stop_flag = False

	def __iter__(self):
		if self._itr == None:
			self._itr = iter(self._clock)
			self._stop_flag = False
		return self

	def __next__(self):
		if self._stop_flag == True:
			raise StopIteration
		try:
			dt, event = next(self._itr)
			self._prev_dt = dt
			return dt, event
		except StopIteration:
			'''generates a FINISH event to signal the end of the back test'''
			self._stop_flag = True
			self._itr = None
			return self._prev_dt + timedelta(minutes=1), FINISH


def create_clock(trading_calendar, sim_params, realtime=False):
	"""
	Factory function for creating a clock.
	"""
	sessions = sim_params.sessions
	trading_o_and_c = trading_calendar.schedule.ix[sessions]
	market_closes = trading_o_and_c['market_close']
	minutely_emission = False

	if sim_params.data_frequency == 'minute':
		market_opens = trading_o_and_c['market_open']

		minutely_emission = sim_params.emission_rate == "minute"
	else:
		# in daily mode, we want to have one bar per session, timestamped
		# as the last minute of the session.
		market_opens = market_closes

	# The calendar's execution times are the minutes over which we actually
	# want to run the clock. Typically the execution times simply adhere to
	# the market open and close times. In the case of the futures calendar,
	# for example, we only want to simulate over a subset of the full 24
	# hour calendar, so the execution times dictate a market open time of
	# 6:31am US/Eastern and a close of 5:00pm US/Eastern.
	execution_opens = \
		trading_calendar.execution_time_from_open(market_opens)
	execution_closes = \
		trading_calendar.execution_time_from_close(market_closes)

	cal = trading_calendar
	t = datetime.combine(date.min, cal.open_time) - timedelta(minutes=15)
	# FIXME generalize these values (update: changed, this, but not sure if it is correct...)
	before_trading_start_minutes = days_at_time(
		sessions,
		t.time(),
		cal.tz
	)
	return MinuteClockWrapper(sessions[-1], se.MinuteSimulationClock(
		sessions,
		execution_opens,
		execution_closes,
		before_trading_start_minutes,
		minute_emission=minutely_emission,
	))
