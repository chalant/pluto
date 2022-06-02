from zipline.finance import order


class Order(order.Order):
    __slots__ = ["id", "dt", "reason", "created", "asset", "amount", "filled",
                 "commission", "_status", "stop", "limit", "stop_reached",
                 "limit_reached", "direction", "type", "broker_order_id"]

    def handle_split(self, ratio):
        # update the amount, limit_price, and stop_price
        # by the split's ratio

        # info here: http://finra.complinet.com/en/display/display_plain.html?
        # rbid=2403&element_id=8950&record_id=12208&print=1

        # new_share_amount = old_share_amount / ratio
        # new_price = old_price * ratio

        # don't convert to int for fractional amounts
        self.amount = self.amount / ratio

        if self.limit is not None:
            self.limit = round(self.limit * ratio, 2)

        if self.stop is not None:
            self.stop = round(self.stop * ratio, 2)
