class EventID(object):
    __slots__ = ['__event_name', '__event_index']

    def __init__(self, name, idx):
        self.__event_name = name
        self.__event_index = idx

    @property
    def event_name(self):
        return self.__event_name

    @property
    def event_index(self):
        return self.__event_index


class Event(object):
    def __init__(self):
        self._observers = []

    def trigger(self, signal):
        for obs in self._observers:
            obs(signal)

    def __iadd__(self, other):
        if other not in self._observers:
            self._observers.append(other)

        return self

    def __isub__(self, other):
        self._observers.remove(other)

        return self


class _EventSystem(object):
    def __init__(self):
        self._events = {}
        self._idx = {}

    def add_event(self, event_name):
        if event_name not in self._events:
            self._events[event_name] = Event()
            self._idx[event_name] = -1
        else:
            raise KeyError("Event {} already exists".format(event_name))

    def remove_event(self, event_name):
        self._events.pop(event_name, None)

    def subscribe(self, event_name, fn):
        event = self._events.get(event_name, None)

        if not event:
            raise KeyError("Event {} does not exist".format(event_name))

        self._idx[event_name] += 1

        event += fn

        return EventID(event_name, self._idx[event_name])

    def trigger(self, event_name, data):
        self._events[event_name].trigger(data)


_manager = _EventSystem()


def add_event(event_name):
    _manager.add_event(event_name)


def remove_event(event_name):
    _manager.remove_event = event_name


def subscribe(event_name, fn):
    _manager.subscribe(event_name, fn)


def post_event(event_name, data):
    _manager.trigger(event_name, data)
