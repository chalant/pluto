import grpc

def check_credentials(func):
    '''decorator function for checking credentials...
    should return a token (that will be re-newed at some frequency...'''
    def wrapper(instance, request, context, username, password):
        metadata = dict(context.invocation_metadata())
        if 'username' not in metadata and 'password' not in metadata:
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            raise AttributeError

        elif metadata['username'] != 'james' and metadata['password'] != 'james':
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            raise AttributeError

        else:
            pass
        return func

def check_token(func):
    def wrapper(instance, request, context):
        metadata = dict(context.invocation_metadata())
        if 'token' not in metadata:
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
        elif metadata['token'] != None:
            pass


def send_session_id(session_id, context):
    context.send_initial_metadata((('session_id',session_id),))

def data_size_info(total_size, chunk_size):
    return (('total_size', total_size), ('chunk_size', chunk_size))

