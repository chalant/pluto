from zipline.finance import order

class Order(order.Order):
    @staticmethod
    def make_id():
        return