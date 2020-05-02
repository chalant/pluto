import uuid
import hashlib

import grpc

_framework_id = hashlib.sha3_512().hexdigest()

_metadata = (('framework_id', _framework_id),)

def invoke(method, request):
    return method(request, metadata=_metadata)

def framework_method(func):
    def wrapper(instance, request, metadata):
        metadata += _metadata
        return func(instance, request, metadata=metadata)
    return wrapper

def framework_only(func):
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
            return func(instance, request, session_id)
    return wrapper

