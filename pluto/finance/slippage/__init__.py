import abc

from zipline.finance import slippage

from protos import slippage_pb2

class Factory(abc.ABC):
    def __call__(self, parameters_path, universe):
        return self._create_model(
            parameters_path,
            universe)

    @abc.abstractmethod
    def _create_model(self, parameters, universe):
        raise NotImplementedError


class FixedPointBasis(Factory):
    def _create_model(self, parameters_path, universe):
        return slippage.FixedBasisPointsSlippage()


class VolumeShare(Factory):
    def _create_model(self, parameters_path, universe):
        return slippage.VolumeShareSlippage()


class Fixed(Factory):
    def _create_model(self, parameters_path, universe):
        return slippage.FixedSlippage()


class EquityShare(Factory):
    def _create_model(self, parameters_path, universe):
        return slippage.EquitySlippageModel()


class FutureShare(Factory):
    def _create_model(self, parameters_path, universe):
        return slippage.FutureSlippageModel()


class VolatilityVolumeShare(Factory):
    def _create_model(self, parameters_path, universe):
        # slippage model: the storage format depends on the slippage model
        # ex: some might store "weights" for a function etc.
        with open(parameters_path) as f:
            arguments = slippage_pb2.VolatilityVolumeShare()
            arguments.ParseFromString(f.read())
            # todo: filter the eta keys using universe so that we only load what we are
            # going to use
            return slippage.VolatilityVolumeShare(
                arguments.volume_limit,
                arguments.eta)


_TYPES = [
    {
        'equity': [
            'volume_share',
            'fixed_point_basis'
        ]
    },
    {
        'future': [
            'volatility_volume_share'
        ]
    }
]

_MODELS = {}

_FACTORIES = {
    'future': {
        'volatility_volume_share': VolatilityVolumeShare()
    },
    'equity': {
        'volume_share': VolumeShare(),
        'fixed_point_basis': FixedPointBasis()
    }
}


def get_slippage_model(setup, parameters_path, universe):
    model = setup.type
    m = _MODELS.get(model, None)
    if not m:
        asset_type = setup.asset_type
        atf = _FACTORIES.get(asset_type, None)
        if not atf:
            raise ValueError(
                '{} is not a supported asset type')
        else:
            factory = atf[model]
            if not factory:
                raise ValueError(
                    '{} slippage model is not supported for {} asset_type')
            else:
                _MODELS[model] = m = _FACTORIES[setup.asset_type][model](
                    parameters_path,
                    universe)
                return m
    else:
        return m
