import grpc

handlers = (
    grpc.unary_stream_rpc_method_handler,
    grpc.unary_unary_rpc_method_handler,
    grpc.stream_unary_rpc_method_handler,
    grpc.stream_stream_rpc_method_handler
)

interceptors = ('availability', 'authenticity')


def unavailable(request, context):
    context.abort(grpc.StatusCode.UNAVAILABLE)

def unauthenticated(request, context):
    context.abort(grpc.StatusCode.UNAUTHENTICATED)

class Authenticity(grpc.ServerInterceptor):
    def __init__(self, method_handler):
        self._token = None
        self._method_handler = method_handler

    @property
    def token(self):
        return

    @token.setter
    def token(self, value):
        self._token = value

    def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method
        # any method that is not Login must requires a token.
        if method != '/interface.Account/Login':
            metadata = dict(handler_call_details.invocation_metadata)
            try:
                expected_token = self._token
                token = metadata['token']
                if not expected_token:
                    return self._method_handler(unauthenticated)
                elif expected_token == token:
                    return continuation(handler_call_details)
                else:
                    return self._method_handler(unauthenticated)
            except KeyError:
                return self._method_handler(unauthenticated)
        else:
            return continuation(handler_call_details)


class Availability(grpc.ServerInterceptor):
    def __init__(self, method_handler):
        self._available = False
        self._method_handler = method_handler

    @property
    def available(self):
        return self._available

    @available.setter
    def available(self, value):
        '''

        Parameters
        ----------
        value : bool

        Returns
        -------
        None
        '''
        self._available = value

    def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method
        # any method that is not Login must requires a token.
        if method != '/interface.Account/Login':
            if not self._available:
                return self._method_handler(unavailable)
            else:
                return continuation(handler_call_details)
        else:
            return continuation(handler_call_details)


def get_interceptors(interceptor_type):
    '''

    Parameters
    ----------
    role : str
        role

    Returns
    -------
    tuple
    '''

    if interceptor_type == 'authenticity':
        return (Authenticity(handler) for handler in handlers)
    elif interceptor_type == 'availability':
        return (Availability(handler) for handler in handlers)
    else:
        raise ValueError('Interceptor type must be one of {}'.format(interceptors))
