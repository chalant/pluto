from typing import Iterable

class _NoneIterator(object):
    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration


class _AssertCollectionType(object):
    def __init__(self, root_type):
        if not isinstance(root_type, type):
            raise TypeError('Expected an argument of type {} but got {}'.format(
                type,
                type(root_type)))
        self._rt = root_type
        self._stack = None
        self._trace = None

    def _check_instance(self, type_):
        if not isinstance(type_, (list, tuple)):
            raise TypeError("Expected arguments of type {} or {}".format(
                list,
                tuple))
        if not isinstance(type_, self._rt):
            if self._stack is None:
                self._stack = []
                self._stack.append(iter(type_))
                # stores types for retracing steps
                self._trace = []
                self._trace.append(type(type_))
            try:
                t = self._stack[-1]
                try:
                    self._next(t)
                except StopIteration:
                    self._stack.pop()
                    self._check_instance(type_)
            except IndexError:
                # means we've finished
                self._stack = None
                self._trace = None

    def _next(self, current):
        # peek iterable stack
        n = next(current)
        self._trace.append(type(n))
        if isinstance(n, str):
            # we've reached bottom (special case to avoid iterating a string)
            self._check_inner_instance(n, self._copy_trace())
            # skip it
            itr = _NoneIterator()
        elif isinstance(n, Iterable):
            itr = iter(n)
        else:
            self._check_inner_instance(n, self._copy_trace())
            itr = _NoneIterator()
        self._stack.append(itr)
        return self._next(itr)

    def _copy_trace(self):
        f = self._trace.copy()
        self._trace.pop()
        return f

    def _repr(self, from_, initial=None):
        try:
            if not initial:
                a = from_.pop()
                b = from_.pop()
                initial = '<{}{}>'.format(b, a)
            else:
                initial = '<{}{}>'.format(from_.pop(), initial)
            return self._repr(from_, initial)
        except IndexError:
            return initial

    def _check_inner_instance(self, type_, from_):
        rt = self._rt
        if not isinstance(type_, rt):
            l = from_.copy()
            l.pop()
            self._stack = None
            self._trace = None
            raise TypeError("Expected arguments of type {} but got {}".format(
                self._repr(l, rt),
                self._repr(from_)))

    def __call__(self, func):
        def assertion(obj, value):
            self._check_instance(value)
            func(obj, value)

        return assertion


class _AssertType(object):
    def __init__(self, type_):
        if not isinstance(type_, type):
            raise TypeError('')
        self._type = type_

    def __call__(self, func):
        t = self._type

        def assertion(obj, input_):
            if t is not type(input_):
                raise TypeError('')
            func(obj, input_)

        return assertion

def assert_collection_type(root_type):
    return _AssertCollectionType(root_type)

def assert_type(type_):
    return _AssertType(type_)