from pluto.control.modes.processes import process_factory

class PodProcess(process_factory.Process):
    pass

class PodProcessFactory(process_factory.ProcessFactory):
    def _create_process(self, framework_url, session_id, root_dir):
        #todo:
        # we can terminate the pod (kubectl delete pod) or through api:
        # /api/v1/namespaces/{namespace}/pods/{name}
        # when deploying a pod, we should give it a unique name that we can use to make
        # delete, exec calls, etc. => The pods shall be named by session_id
        # controllables are made such that they can be restarted when they fail, and resume
        # from where they left-off
        # we will need a "service" to be able to call the pod, since it might restart, it will
        # get a new ip address, so we need a service. or we could find the node using its name,
        # then get its cluster-ip => this is the preferred approach PROBLEM: we might need to do
        # this on each iteration... at least we have more control over the "refresh" frequency
        pass