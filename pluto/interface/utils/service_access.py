import uuid
import hashlib

import grpc

_framework_id = hashlib.sha3_512().hexdigest()

_session_ids = {}

_metadata = (('framework_id', _framework_id),)


def add_session_id(session_id):
    _session_ids[session_id] = session_id


def with_session_id(class_):
    def init(self, session_id, *args, **kwargs):
        self._session_id = session_id
        class_.__init__(*args, *kwargs)

    class_.__init__ = init

    return init


def invoke(method, request):
    return method(request, metadata=_metadata)


def with_metadata(access_types):
    def decorator(fn):
        def wrapper(instance, *args, **kwargs):
            for access in access_types:
                if access == 'framework':
                    kwargs['metadata'] += _metadata
                if access == 'session':
                    kwargs['metadata'] += instance.session_id
            fn(instance, *args, **kwargs)
        return wrapper
    return decorator


def framework_method(func):
    def wrapper(instance, request, metadata):
        metadata += _metadata
        return func(instance, request, metadata=metadata)
    return wrapper


def framework(func):
    def wrapper(instance, request, context):
        metadata = dict(context.invocation_metadata())
        framework_id = metadata.get('framework_id', None)
        if not framework_id:
            context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                "reserved for internal use only")
        else:
            if _framework_id != framework_id:
                context.abort(
                    grpc.StatusCode.PERMISSION_DENIED,
                    "reserved for internal use only")
            else:
                return func(instance, request, context)
    return wrapper


def session_method(func):
    def wrapper(instance, request, metadata):
        metadata += (('session_id', instance.session_id),)
        return func(instance, request, metadata=metadata)
    return wrapper


def per_session(func):
    def wrapper(instance, request, context):
        meta = dict(context.invocation_metadata())
        session_id = meta.get('session_id', None)
        if not session_id:
            context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                'must specify session_id')
        else:
            if session_id not in _session_ids:
                context.abort(
                    grpc.StatusCode.PERMISSION_DENIED,
                    'access to this service is forbidden')
            else:
                return func(instance, request, session_id)
    return wrapper

