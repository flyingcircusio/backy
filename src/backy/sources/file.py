

class File(object):

    def __init__(self, config):
        self.filename = config['filename']

    @staticmethod
    def config_from_cli(spec):
        return dict(filename=spec)
