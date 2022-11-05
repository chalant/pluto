class AccountSetup(object):
    def __init__(self, capital, max_leverage):
        self.__capital = capital
        self.__max_leverage = max_leverage

    @property
    def capital(self):
        return self.__capital

    @property
    def max_leverage(self):
        return self.__max_leverage
