from backy.utils import safe_copy


class File(object):

    def __init__(self, config):
        self.filename = config['filename']

    @staticmethod
    def config_from_cli(spec):
        return dict(filename=spec)

    def backup(self, revision):
        target = open(revision.filename, 'wb')
        source = open(self.filename, 'rb')

        revision.stats['bytes_written'] = 0
        for chunk in safe_copy(source, target):
            revision.stats['bytes_written'] += len(chunk)
