import grpc

from protos import controller_pb2_grpc
from protos import controller_pb2


class ControllerService(controller_pb2_grpc.ControllerServicer):
    def __init__(self, directory, controller):
        '''

        Parameters
        ----------
        directory
        controller:
        '''
        self._directory = directory
        self._controller = controller

    def Run(self, request, context):
        with self._directory.read() as d:
            # we should flag a strategy if its back-test was successful (without errors).

            # todo: if this method is called while it is running, it will add the sessions to the loop
            #  (in live mode), won't do anything in dev? => each type of service has a different way
            #  of handling multiple calls...

            ctl = self._controller
            try:
                ctl.run(request.mode, d, request.run_params)
            except RuntimeError as e:
                # the loop might raise a runtime error if it doesn't support
                # running additional strategies while running...
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details(str(e))
            except KeyError as e:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(str(e))
        return controller_pb2.RunResponse()

    def Stop(self, request, context):
        pass
