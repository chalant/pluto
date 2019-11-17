import grpc

def unauthenticated(request, context):
    context.abort(grpc.StatusCode.UNAUTHENTICATED)

class CredentialsValidator(grpc.ServerInterceptor):
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
        #any method that is not Login must requires a token.
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