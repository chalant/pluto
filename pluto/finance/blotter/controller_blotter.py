class OrderTransactions(object):
    __slots__ = ['commissions', 'transactions', 'order']

    def __init__(self, order):
        self.order = order
        self.transactions = []
        self.commissions = []

