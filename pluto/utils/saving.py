import abc

class Savable(abc.ABC):
    @abc.abstractmethod
    def get_state(self, dt):
        """
        Parameters
        ----------

        dt : pandas.Timestamp

        Returns
        -------
        bytes

        """
        raise NotImplementedError

    def restore_state(self, state):
        """

        Parameters
        ----------
        state : bytes

        Returns
        -------
        pandas.Timestamp
        """
        if not isinstance(state, bytes):
            raise TypeError('Expected an argument of type {} got {}'.format(bytes, type(state)))
        return self._restore_state(state)

    @abc.abstractmethod
    def _restore_state(self, state):
        raise NotImplementedError
