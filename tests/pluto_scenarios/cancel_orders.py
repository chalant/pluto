"""Order cancellation: open limit orders that do not fill are cancelled with
cancel_order on a later bar; get_open_orders drives the cancellation; a
marketable order cancelled in the bar it is placed must never fill."""
from pluto.api import *

from tests.pluto_scenarios import common


def initialize(context):
    common.set_models(common.default_commission, common.default_slippage)
    context.aapl = symbol('AAPL')
    context.msft = symbol('MSFT')
    context.i = 0


def handle_data(context, data):
    context.i += 1
    i = context.i
    if i == 1:
        price = data.current(context.aapl, 'price')
        context.order_id = order(context.aapl, 100, limit_price=price * 0.9)
        order(context.msft, 100, limit_price=data.current(context.msft, 'price') * 0.9)
    elif i == 2:
        price = data.current(context.aapl, 'price')
        cancel_order(order(context.aapl, 10, limit_price=price * 2.0))
    elif i == 4:
        cancel_order(context.order_id)
    elif i == 6:
        for asset, orders in get_open_orders().items():
            for o in orders:
                cancel_order(o)
    elif i == 8:
        order(context.msft, 50)


def _test_args():
    return common.test_args(
        common.default_commission, common.default_slippage,
        '2014-01-02', '2014-02-28')
