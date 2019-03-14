import abc

class Memento(abc.ABC):
    """Encapsulates a method for storing state"""
    def __init__(self, state):
        self._state = state

    def store_state(self, path):
        self._write(path, self._state)

    @abc.abstractmethod
    def _write(self, path, state):
        raise NotImplementedError

class ByteMemento(Memento):
    def _write(self, path, state):
        with open(path, "wb") as f:
            f.write(state)

class Savable(abc.ABC):
    @abc.abstractmethod
    def get_memento(self, dt):
        """
        Parameters
        ----------

        dt : pandas.Timestamp

        Returns
        -------
        Memento

        """
        raise NotImplementedError

    @abc.abstractmethod
    def restore_state(self, memento):
        """

        Parameters
        ----------
        memento : Memento

        """
        raise NotImplementedError
