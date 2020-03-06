import uuid

import grpc

_framework_id = uuid.uuid4().hex

_metadata = (('framework_id', _framework_id))

def invoke(method, request):
    method(request, metadata=_metadata)

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
                func(instance, request, context)
    return wrapper