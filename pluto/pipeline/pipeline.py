from zipline.pipeline import pipeline, domain as dom


class Pipeline(pipeline.Pipeline):
    def __init__(self, columns=None, screen=None, domain=None):
        # any usage of domain is ignored
        super(Pipeline, self).__init__(columns, screen, domain=dom.GENERIC)