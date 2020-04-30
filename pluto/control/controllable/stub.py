import abc

from pluto.interface.utils import method_access

def _invoke(func):
    def wrapper(instance, request, metadata=None):
        return func(
            instance,
            request,
            context=instance._get_context(metadata))
    return wrapper

class ControllableStub(object):
    @method_access.framework_method
    def Initialize(self, request, metadata=()):
        return self._initialize(request, metadata)

    @method_access.framework_method
    def UpdateParameters(self, request, metadata=()):
        return self._update_parameters(request, metadata)

    @method_access.framework_method
    def ClockUpdate(self, request, metadata=()):
        return self._clock_update(request, metadata)

    @method_access.framework_method
    def Stop(self, request, metadata=()):
        return self._clock_update(request, metadata)

    @method_access.framework_method
    def UpdateAccount(self, request, metadata=()):
        return self._update_account(request, metadata)

    @method_access.framework_method
    def Watch(self, request, metadata=None):
        return self._watch(request, metadata)

    @method_access.framework_method
    def StopWatching(self, request, metadata=()):
        return self._stop_watching(request, metadata)

    @abc.abstractmethod
    def _initialize(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _update_parameters(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _clock_update(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _stop(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _update_account(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _watch(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _stop_watching(self, request, metadata):
        raise NotImplementedError
