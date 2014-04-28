import re


class Ellipsis(object):

    def __init__(self, ellipsis):
        self.ellipsis = ellipsis
        pattern = re.escape(ellipsis)
        pattern = pattern.replace('\.\.\.', '.+')
        self.pattern = re.compile(pattern)

    def __eq__(self, other):
        if not self.ellipsis and other:
            return False
        assert isinstance(other, str)
        if self.pattern.match(other):
            return True
        return False
