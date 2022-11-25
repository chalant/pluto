from abc import abstractmethod, ABC


class AbstractBundleIngester(ABC):
    def __init__(self, name, root_dir):

        self.name = name
        self._root = root_dir.get_directory('data').add_directory(name)

    def ingest(self, *args, **kwargs):
        self._ingest(self._root)

    @abstractmethod
    def _ingest(self, root_dir, *args, **kwargs):
        pass


class AbstractBundleLoader(ABC):
    def load(self):
        return self._load()

    @abstractmethod
    def _load(self):
        pass
