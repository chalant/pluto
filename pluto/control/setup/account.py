class AccountSetup(object):
    def __init__(self, capital):
        self.__capital = capital

    @property
    def capital(self):
        return self.__capital
