import abc
from abc import abstractmethod, ABC


class AbstractBundleIngester(ABC):
    def __init__(self,
                 name,
                 root_dir,
                 calendar_name,
                 start_session=None,
                 end_session=None,
                 minutes_per_day=390):

        self.name = name
        self._root = root_dir.get_directory('data').add_directory(name)

        self.calendar_name = calendar_name
        self.start_session = start_session
        self.end_session = end_session
        self.minutes_per_day = minutes_per_day

    def ingest(self):
        self._ingest(self._root)

    @abstractmethod
    def _ingest(self, root_dir):
        pass


class AbstractBundleLoader(ABC):
    def load(self):
        return self._load()

    @abstractmethod
    def _load(self):
        pass
