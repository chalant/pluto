def load(path):
    namespace = {}

    with open(path) as f:
        script = f.read()

    code = compile(script, getattr(script, 'name', '<algorithm>'), 'exec')

    exec(code, namespace)

    return ScriptHandler(
        namespace
    )


class ScriptHandler(object):
    def __init__(self, namespace):
        # todo: maybe raise an error when a function is not implemented ?
        def noop(*args, **kwargs):
            pass

        self._setup = namespace.get('setup', noop)

    def setup(self, context):
        self._setup(context)
