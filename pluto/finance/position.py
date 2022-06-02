import numpy as np

import logbook

from zipline.finance import position
import zipline.protocol as zp

log = logbook.Logger('Performance')


class Position(position.Position):
    __slots__ = 'inner_position', 'protocol_position', 'minimal_fractional_amount'

    def __init__(self,
                 asset,
                 amount=0.0,
                 cost_basis=0.0,
                 last_sale_price=0.0,
                 last_sale_date=None,
                 minimal_fractional_amount=0.0):
        super(Position, self).__init__(
            asset, amount, cost_basis,
            last_sale_price, last_sale_date)

        inner = zp.InnerPosition(
            asset=asset,
            amount=amount,
            cost_basis=cost_basis,
            last_sale_price=last_sale_price,
            last_sale_date=last_sale_date,
        )
        object.__setattr__(self, 'inner_position', inner)
        object.__setattr__(self, 'protocol_position', zp.Position(inner))

        self.minimal_fractional_amount = minimal_fractional_amount

    def earn_stock_dividend(self, stock_dividend):
        # override the base amount to support fractional shares

        """
        Register the number of shares we held at this dividend's ex date so
        that we can pay out the correct amount on the dividend's pay date.
        """
        return {
            'payment_asset': stock_dividend.payment_asset,
            'share_count': self.amount * float(stock_dividend.ratio)
        }

    def handle_split(self, asset, ratio):
        """
                Update the position by the split ratio, and return the resulting
                fractional share that will be converted into cash.

                Returns the unused cash.
                """
        if self.asset != asset:
            raise Exception("updating split with the wrong asset!")

        # todo: handle fractional shares. fractional shares are allowed

        # adjust the # of shares by the ratio
        # (if we had 100 shares, and the ratio is 3,
        #  we now have 33 shares)
        # (old_share_count / ratio = new_share_count)
        # (old_price * ratio = new_price)

        # e.g., 33.333
        raw_share_count = self.amount / float(ratio)

        # e.g., 33
        full_share_count = np.floor(raw_share_count)

        # todo: what is the minimal amount of fractional shares ?
        #  we can determine the minimal amount by using the minimal
        #  value one can invest (ex: 1 USD) if the remaining shares don't
        #  add up to that sum, we return the cash in cents.

        whole_amount = np.floor(self.amount)
        fractional_amount = self.amount - whole_amount

        # if we have a fractional amount of assets, we won't return cash

        # todo: we need
        if fractional_amount > self.minimal_fractional_amount:
            fractional_share_count = 0.0
        else:
            # e.g., 0.333
            fractional_share_count = raw_share_count - full_share_count

        # adjust the cost basis to the nearest cent, e.g., 60.0
        new_cost_basis = round(self.cost_basis * ratio, 2)

        self.cost_basis = new_cost_basis
        self.amount = full_share_count

        return_cash = round(float(fractional_share_count * new_cost_basis), 2)

        log.info("after split: " + str(self))
        log.info("returning cash: " + str(return_cash))

        # return the leftover cash, which will be converted into cash
        # (rounded to the nearest cent)
        return return_cash



