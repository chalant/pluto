import yaml

from google import protobuf

from protos import structs_pb2

from pluto.setup import directory_setup

import structs

from zipline.assets import Equity, Future

from zipline.finance import cancel_policy

from zipline.finance import commission
from zipline.finance import slippage


def commission_from_struct(data):
    obj = structs_pb2.Struct()

    obj.ParseFromString(data.value)

    # todo: we need to parse fields recursively

    fields = {}

    primitive = structs_pb2.Primitive()
    struct = structs_pb2.Struct()

    # todo: bad parsing! (temporary!)

    for field in obj.fields:
        if field.is_primitive:
            primitive.ParseFromString(field.value)
        else:
            struct.ParseFromString(field.value)
            primitive.ParseFromString(struct.fields[0].value)

        fields[field.name] = eval(primitive.value)

    if obj.name == "PerShare":
        return commission.PerShare(
            fields['cost_per_share'],
            fields['min_trade_cost'])

    elif obj.name == "PerContract":
        return commission.PerContract(
            fields['_cost_per_contract'],
            fields['_exchange_fee'],
            fields['min_trade_cost']
        )

    elif obj.name == "PerTrade":
        return commission.PerTrade(
            fields['cost']
        )

    elif obj.name == "PerFutureTrade":
        return commission.PerFutureTrade(
            fields['_cost_per_trade']
        )

    elif obj.name == "PerDollar":
        return commission.PerDollar(
            fields['cost_per_dollar']
        )

    elif obj.name == "NoCommission":
        return commission.NoCommission()

    else:
        raise NotImplemented(
            "No commission model of type {}".format(obj.name))


def slippage_from_struct(data):
    obj = structs_pb2.Struct()

    obj.ParseFromString(data.value)

    fields = {}

    primitive = structs_pb2.Primitive()
    struct = structs_pb2.Struct()

    # todo: bad parsing! (temporary!)

    for field in obj.fields:
        if field.is_primitive:
            primitive.ParseFromString(field.value)
        else:
            struct.ParseFromString(field.value)
            primitive.ParseFromString(struct.fields[0].value)

        try:
            fields[field.name] = eval(primitive.value)
        except SyntaxError:
            pass

    if obj.name == "VolumeShareSlippage":
        return slippage.VolumeShareSlippage(
            fields['volume_limit'],
            fields['price_impact']
        )

    elif obj.name == "FixedBasisPointsSlippage":
        return slippage.FixedBasisPointsSlippage(
            fields['basis_points'],
            fields['volume_limit'])

    elif obj.name == "FixedSlippage":
        return slippage.FixedSlippage(
            fields['spread'])

    elif obj.name == "VolatilityVolumeShare":
        return slippage.VolatilityVolumeShare(
            fields['volume_limit'],
            fields['_eta'])

    elif obj.name == "NoSlippage":
        slippage.NoSlippage()

    else:
        raise NotImplementedError(
            "No slippage model of type {}".format(obj.name))


class EngineSetup(object):
    def __init__(self):
        self.commission_models = {}
        self.capital = 1000000
        self.tax_models = {}
        self.trading_universe = "test"
        self.slippage_models = {}

    def get_commission_model(self, asset):
        return self.commission_models[type(asset)]

    def get_slippage_model(self, asset):
        return self.slippage_models[type(asset)]


def load_engine_setup(
        dir_,
        universe_name,
        capital,
        eng_stp):

    stp = EngineSetup()
    stp.capital = capital

    # todo: this should load the trading universe.
    stp.trading_universe = universe_name

    # todo: load setup
    # commission_models.equity
    # tax_models.equity

    # with open(dir_.get_directory('universes').get_file("universes", "yaml").path) as f:
    #     uni = yaml.load(f, Loader=yaml.FullLoader)
    #
    # setup_dir = dir_.get_directory('setup')
    #
    # with open(setup_dir.get_file("tax_models", "yaml").path) as f:
    #     tm = yaml.load(f, Loader=yaml.FullLoader)
    #
    # with open(setup_dir.get_file("commission_models", "yaml").path) as f:
    #     # todo: load and create commission models
    #     cm = yaml.load(f, Loader=yaml.FullLoader)

    stp.commission_models = {
        Equity: commission_from_struct(eng_stp.commission_models['equity']),
        Future: commission_from_struct(eng_stp.commission_models['futures'])
    }

    stp.slippage_models = {
        Equity: slippage_from_struct(eng_stp.slippage_models['equity']),
        Future: slippage_from_struct(eng_stp.slippage_models['futures'])
    }


    # todo: set models

    return stp
