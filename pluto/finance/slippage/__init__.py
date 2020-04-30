import abc

from zipline.finance import slippage

from protos import slippage_pb2

class Factory(abc.ABC):
    def __call__(self, parameters_path, universe=None):
        return self._create_model(
            parameters_path,
            universe)

    @abc.abstractmethod
    def _create_model(self, parameters, universe=None):
        raise NotImplementedError


class FixedPointBasis(Factory):
    def _create_model(self, parameters_path, universe=None):
        return slippage.FixedBasisPointsSlippage()


class VolumeShare(Factory):
    def _create_model(self, parameters_path, universe=None):
        return slippage.VolumeShareSlippage()


class Fixed(Factory):
    def _create_model(self, parameters_path, universe=None):
        return slippage.FixedSlippage()


class EquityShare(Factory):
    def _create_model(self, parameters_path, universe=None):
        return slippage.EquitySlippageModel()


class FutureShare(Factory):
    def _create_model(self, parameters_path, universe=None):
        return slippage.FutureSlippageModel()


class VolatilityVolumeShare(Factory):
    def _create_model(self, parameters_path, universe=None):
        try:
            params = _PARAMS_CACHE[parameters_path]
        except KeyError:
            with open(parameters_path) as f:
                params = slippage_pb2.VolatilityVolumeShare()
                params.ParseFromString(f.read())
                # todo: filter the eta keys using universe so that we only load what we are
                # going to use
                _PARAMS_CACHE[parameters_path] = params
        finally:
            # todo: filter eta based on the universe assets if the universe is provided
            return slippage.VolatilityVolumeShare(
                    params.volume_limit,
                    params.eta)


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

_FACTORIES = {
    'future': {
        'volatility_volume_share': VolatilityVolumeShare()
    },
    'equity': {
        'volume_share': VolumeShare(),
        'fixed_point_basis': FixedPointBasis()
    }
}

_PARAMS_CACHE = {

}

def get_slippage_model(setup, parameters_path, universe=None):
    model = setup.type
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
            return _FACTORIES[setup.asset_type][model](
                parameters_path,
                universe)
