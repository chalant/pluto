import abc
import uuid
import os

from collections import deque

from protos import graph_pb2

from google.protobuf import empty_pb2
from protos import data_pb2


class Store(abc.ABC):
    @abc.abstractmethod
    def load(self, id_, chunk_size=None):
        raise NotImplementedError

    @abc.abstractmethod
    def store(self, id_, path, chunk_size):
        raise NotImplementedError

class Storage(abc.ABC):
    def load(self, id_, path, chunk_size=None):
        for data in self._load(id_, path, chunk_size):
            yield data

    def store(self, id_, path, iterable):
        self._store

    @abc.abstractmethod
    def _load(self, id_, path, chunk_size):
        raise NotImplementedError

    @abc.abstractmethod
    def _store(self, id_, path, iterable):
        raise NotImplementedError

class MultiFileStorage(Storage):
    def _load(self, id_, path, chunk_size):
        pth = os.path.join(path, id_)
        with open(pth, 'rb') as f:
            if chunk_size:
                while True:
                    buffer = f.read(chunk_size)
                    if buffer == b'':
                        break
                    else:
                        yield buffer
            else:
                yield f.read()

class LocalStore(Store):
    def __init__(self, path, storage):
        self._path = path
        self._storage = storage

    def load(self, id_, chunk_size=None):
        for data in self._storage.load(id_, self._path, chunk_size):
            yield data

    def store(self, id_, iterable):
        self._storage.store(id_, self._path, iterable)

    def _store(self, id_, path, iterable):
        pth = os.path.join(path, id_)
        with open(pth, 'wb') as f:
            for data in iterable:
                f.write(data)

class RemoteStore(Store):
    def __init__(self, store_stub):
        self._stub = store_stub

    def store(self, id_, iterable, chunk_size):
        def stream(iterable):
            for b in iterable:
                yield data_pb2.Data(data=b)

        self._stub.StoreData(stream(), metadata=(('id', id_),('chunk_size', chunk_size)))

    def load(self, id_, path, chunk_size):
        for chunk in self._stub.GetData(empty_pb2.Empty(), metadata=(('id', id_), ('chunk_size', chunk_size))):
            yield chunk.data


class GraphDepthIterator(object):
    def __init__(self, graph):
        self._iter_stack = [iter(graph)]

    def __iter__(self):
        return self

    def __next__(self):
        itr_stack = self._iter_stack
        if itr_stack:
            itr = itr_stack[-1]
            try:
                itr_stack.append(iter(next(itr)))
                return self.__next__()
            except StopIteration:
                #return the iterator (which is the element)
                return itr_stack.pop()
        else:
            raise StopIteration

    def previous(self):
        itr_stack = self._iter_stack
        if itr_stack:
            itr_stack.pop()
            return itr_stack[-1]
        else:
            raise StopIteration

class GraphBreadthIterator(object):
    def __init__(self, graph):
        self._queue = queue = deque()
        queue.append(iter(graph))

    def __iter__(self):
        return self

    def __next__(self):
        queue = self._queue
        if queue:
            try:
                queue.appendleft(iter(next(queue[-1])))
            except StopIteration:
                queue.pop()
        else:
            raise StopIteration

#Todo: this is a factory class... should be abstracted
class Builder(object):
    def __init__(self, store):
        self._store = store
        self._elements_by_id = {}
        self._elements_by_name = {}

        #for shortcuts
        self._maps = {}

    def get_element(self, id_):
        return self._elements_by_id[id_]

    def group(self, name, id_=None):
        return self.element('group', name, id_)

    def value(self, name, id_=None):
        return self.element('value', name, id_)

    def file(self, name, id_=None):
        return self.element('file', name, id_)

    def single(self, name, element, id_=None):
        s = self.element('single', name, id_)
        s.set_element(element)
        return s

    def element(self, type_, name, id_=None):
        if id_ is None:
            id_ = uuid.uuid4().hex
        if type_ == 'value':
            el =  Value(name, id_, self._store)
        elif type_ == 'group':
            el = Group(name, id_, self._store)
        elif type_ == 'file':
            el =  File(name, id_, self._store)
        elif type_ == 'single':
            el =  Single(name, id_, self._store)
        else:
            raise AttributeError

        self._elements_by_id[id_] = el

        #todo: do we need to classify elements by name?
        els = self._elements_by_name.get(name,None)
        if els:
            els.append(el)
        else:
            self._elements_by_name[name] = [el]

# these elements are used for indexing, not storing and loading
# they don't know what they are storing. This is known by the client.
# the client knows what to expect when loading an indexed element.

class Element(abc.ABC):
    '''a factory class that returns storable objects'''
    def __init__(self, value, id_, store):
        self._name = value
        self._id = id_
        self._str = store
        self._parents = []
        #the origin indicates where the element came from...
        #if we make a copy, the new element will have the orginal element
        #as origin...
        self._origin = None
        self._copy = False
        self._relations = {}

    def join(self, key, value):
        self._relations[key] = value

    def relation(self, key):
        return self._relations[key]

    @property
    def copy(self):
        return self._copy

    @property
    def origin(self):
        return self._origin

    @origin.setter
    def origin(self, value):
        self._copy = True
        self._origin = value

    @property
    def id(self):
        self._id

    @property
    def value(self):
        return self._name

    @abc.abstractproperty
    @property
    def type(self):
        raise NotImplementedError

    @property
    def parents(self):
        return self._parents

    def load(self, chunk_size=None, headless=False):
        if not headless:
            yield 'HEADER'
            #todo: use bytes io to cut this in chunks of fixed size...
            yield to_bytes(graph_pb2.Node(name=self.value, type=self.name, id=self.id, origin=self.origin))
            yield b'BODY'
            for chunk in self._load(self._str, chunk_size):
                yield chunk
            yield b'END'
        else:
            for chunk in self._load(self._str, chunk_size):
                yield chunk

    def delete(self):
        self._delete(self._store)

    def _delete(self, store):
        raise NotImplementedError

    @abc.abstractmethod
    def _load(self, store, chunk_size):
        raise NotImplementedError

    @abc.abstractmethod
    def get_element(self, value):
        '''returns the element that contains some value or the element
        that matches the name.'''
        raise NotImplementedError


    def get_group(self, value, parent=None):
        '''The parent is passed as argument to specify which parent we need
        to return, since an element can have multiple parents'''
        if parent:
            if self._parents:
                self._get_group(value, parent)
            else:
                return [self]
        else:
            return self.get_element(value)


    @abc.abstractmethod
    def _get_group(self, value, parent):
        raise NotImplementedError

    def _add_parent(self, parent):
        self._parents.append(parent)

class Item(Element):
    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration

    def get_element(self, value):
        if self._name == value:
            return [self]
        else:
            return []

    def store(self, iterable):
        self._str.store(self._id, iterable)

    def _get_group(self, value, parent=None):
        if self._name == value:
            return [parent]
        else:
            return []

#an element that stores a value in memory
class Value(Item):
    #todo: a value must be unique, since we use this to find elements that share the same value..
    # an element that is unique and can have multiple parents...
    def type(self):
        return 'value'

    def _load(self, store, chunk_size):
        yield self._name.encode()

class File(Item):
    def type(self):
        return 'file'

    def _load(self, store, chunk_size):
        yield store.load(self._id, chunk_size)

    def _delete(self, store):
        store.delete(self._id)

class Single(Element):
    '''Stores a single element by name.'''
    def __init__(self, name, id_, store):
        super(Single, self).__init__(name, id_, store)
        self._element = None

    def type(self):
        return 'single'

    def get_element(self, name):
        if name == self._name:
            return [self]
        else:
            return self._element.get_element(name)

    def _delete(self, store):
        self._element.delete()

        self._element = None

    def set_element(self, value):
        self._element = value

    def __iter__(self):
        return iter(self._element)

class Group(Element):
    '''Stores a group of elements by name'''
    def __init__(self, value, id_, store):
        super(Group, self).__init__(value, id_, store)
        self._elements = {}

    def type(self):
        return 'group'

    def add_element(self, value):
        #sets itself as the parent PROBLEM!: can have multiple parents!
        #so the parent must be set by search not creation.
        value._add_parent(self)
        self._elements[value.value] = value

    def _delete(self, store):
        for element in self._elements:
            element.delete()

        self._elements = []

    def get_element(self, name):
        return self._elements.get(name, None)

    def _get_group(self, value, parent=None):
        if value == self._name:
            if parent:
                if self._parents:
                    return [parent]
                else:
                    return []
            else:
                return []
        else:
            elements = []
            for element in self._elements:
                el = element.get_group(value, self)
                if el:
                    # return extend the list of elements
                    elements.extend(el)
            return elements

    def _load(self, store, chunk_size):
        for element in self._elements.values():
            for data in element.load(chunk_size):
                yield data

    def __iter__(self):
        return iter(self._elements.values())

def to_bytes(proto_def):
    return proto_def.SerializeToString()

def from_bytes(proto_def):
    return proto_def.ParseFromString()

def serialize_graph(graph):
    '''stores the graph structure as a sequence '''
    def create_node(node):
        org = node.origin
        return graph_pb2.Node(name=node.value, type=node.type, id=node.id, origin=create_node(org) if org else None)
    return graph_pb2.Graph([create_node(node) for node in GraphDepthIterator(graph)])

def load_graph(path):
    with open(path, 'rb') as f:
        return create_graph(f.read(path))


class NodeCreator(object):
    def __init__(self, builder):
        self._builder = builder
        self._instance_dict = {}

    def create_node(self, node_def, type_, copy_=False):
        instance_dict = self._instance_dict
        id_ = node_def.id
        # if the type is a value, load it in memory
        if id_ not in instance_dict:
            instance_dict[id_] = el = self._builder.element(type_, node_def.name, id_ if not copy_ else None)
            ori = node_def.origin
            if ori:
                el.origin = self.create_node(ori, ori.type, copy_)  # create the origin element
        else:
            el = instance_dict[id_]
        return el


def create_graph(graph_def, factory, copy_=False):
    '''
    Creates a graph from a serialized graph...
    Parameters
    ----------
    graph_def
    factory

    Returns
    -------

    '''
    sequence = []

    node_creator = NodeCreator(factory)

    for node_def in graph_def:
        t = node_def.type
        el = node_creator.create_node(node_def, t, copy_)
        # append the instance to the sequence
        if node_def.type == 'group':
            # add all the previous elements
            while sequence:
                el.add_element(sequence.pop())
        elif node_def.type == 'single':
            # add the previous element
            el.add_element(sequence.pop())
        sequence.append(el)
        # the root of the tree/graph

        return sequence.pop()

def copy(graph, factory):
    '''
    Copies a graph and returns a new graph...

    Parameters
    ----------
    graph

    Returns
    -------
    Element
    '''
    return create_graph(serialize_graph(graph), factory, copy_=True)


def freeze(graph, path):
    with open(path, "wb") as f:
        f.write(to_bytes(serialize_graph(graph)))


def is_derived_from(source_id, subject):
    org = subject.origin

    if org:
        if source_id == org.id:
            return True
        else:
            return is_derived_from(source_id, org)
    else:
        #if we reach None, then the subject isn't derived from the source.
        return False

def parse_header(stream):
    '''builds a node'''
    while data != b'BODY':
        data = next(stream).data
    #todo: return the graph node.
    return

def ingest_graph(stream, builder):

    node_creator = NodeCreator(builder)

    def body_yield(stream):
        while data != b'END':
            data = next(stream).data
            yield data

    while True:
        try:
            data = next(stream).data
        except StopIteration:
            #todo: store the built graph.
            break

        if data == b'HEADER':
            while data != b'BODY':
                data = next(stream).data
                node_bytes = node_bytes + data  # todo: can this work?
            node_def = graph_pb2.Node()
            node_def.ParseFromString(node_bytes)
            node = node_creator.create_node(node_def, node_def.type)

        if data == b'BODY':
            node.store(body_yield(stream))

def load_graph_from_bytes(iterable, builder):
    gph = graph_pb2.Graph()
    gph_bytes = b''
    for chunk in iterable:
        gph_bytes = gph_bytes + chunk.data
    gph.ParseFromString(gph_bytes)

    return create_graph(gph, builder)

