class MetricsTracker(object):
    _hooks = (
        'start_of_simulation',
        'end_of_simulation',

        'start_of_session',
        'end_of_session',

        'end_of_bar',
    )

    def __init__(self, account, trading_calendar, first_session, metrics):
        #todo: we need a moving window of "sessions" and we need to record the previous
        # values
        #todo: with the default metrics, there is no need for start_of_simulation
        # and end_of_simulation.
        self._account = account

        for hook in self._hooks:
            registered = []
            for metric in metrics:
                try:
                    registered.append(getattr(metric, hook))
                except AttributeError:
                    pass

            def closing_over_loop_variables_is_hard(registered=registered):
                def hook_implementation(*args, **kwargs):
                    for impl in registered:
                        impl(*args, **kwargs)

                return hook_implementation

            hook_implementation = closing_over_loop_variables_is_hard()

            hook_implementation.__name__ = hook
            setattr(self, hook, hook_implementation)

    @property
    def portfolio(self):
        return self._account.portfolio

    @property
    def account(self):
        return self._account.account

    @property
    def positions(self):
        return self._account.positions

    def update(self, dt):
        self._account.update(dt)

    def handle_start(self, benchmark_source, trading_calendar):
        pass

    def handle_minute_close(self, dt, data_portal):
        pass

    def handle_market_open(self, session_label, data_portal, trading_calendar):
        pass

    def handle_market_close(self, dt, data_portal):
        pass

    def handle_end(self, data_portal, trading_calendar):
        pass

