def load(path, main_setup, stg_store):
    namespace = {}

    with open(path) as f:
        script = f.read()

    code = compile(script, getattr(script, 'name', '<algorithm>'), 'exec')

    exec(code, namespace)

    return ScriptHandler(
        namespace,
        main_setup,
        stg_store
    )


class ScriptHandler(object):
    def __init__(self, namespace, stp, stg_store):
        # todo: maybe raise an error when a function is not implemented ?
        def noop(*args, **kwargs):
            pass

        self._setup = namespace.get('setup', noop)
        self._main_setup = stp
        self._stg_store = stg_store

    def setup(self, context):
        self._setup(context)

        main_setup = self._main_setup

        max_capital = main_setup.capital
        max_leverage = main_setup.leverage

        total_capital = 0.0
        total_leverage = 0.0

        for stp, stg in self._stg_store.strategies:
            total_capital += stp.capital
            total_leverage += stp.max_leverage

            if total_capital > max_capital:
                raise ValueError("Total assigned capital exceeds the maximum threshold")
            if total_leverage > max_leverage:
                raise ValueError("Total assigned leverage exceeds the maximum threshold")
