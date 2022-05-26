from abc import abstractmethod, ABC

from zipline.finance import commission


class TaxModel(commission.CommissionModel):
    @abstractmethod
    def calculate(self, order, transaction):
        raise NotImplementedError


class PercentagePerTransaction(TaxModel):
    def __init__(self, percentage, commission_model, ceiling=None):
        self._percentage = percentage
        self._commission_model = commission_model
        self._ceiling = ceiling

    def calculate(self, order, transaction):
        value = self._percentage * transaction.price + self._commission_model.calculate(order, transaction)

        ceiling = self._ceiling

        if ceiling and value > ceiling:
            return ceiling

        return value


class PercentagePerOrder(TaxModel):
    def __init__(self, percentage, commission_model, ceiling=None):
        self._percentage = percentage
        self._commission_model = commission_model
        self._ceiling = ceiling

        self._total_price = 0

    def calculate(self, order, transaction):
        # no tax until the order has been filled.
        if order.filled < order.amount:
            self._total_price += transaction.amount * transaction.price

            return 0.0

        ceiling = self._ceiling

        value = self._total_price * self._percentage

        if ceiling and value > ceiling:
            return ceiling

        return value


