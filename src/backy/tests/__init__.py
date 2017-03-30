import re
import difflib


class Report(object):

    matches = None

    def __init__(self):
        self.lines = []

    def matched(self, line):
        self.lines.append((True, line, None))

    def nonmatched(self, line, pattern):
        self.lines.append((False, line, pattern))

    @property
    def diff(self):
        result = ['']
        d = difflib.Differ()
        for matched, line, pattern in self.lines:
            if matched:
                result.append('  ' + line)
            else:
                if pattern is None:
                    result.append('+ ' + line)
                elif line is None:
                    result.append('- ' + pattern)
                else:
                    diffed = d.compare([pattern], [line])
                    diffed = [x.rstrip('\n') for x in diffed]
                    result.extend(diffed)
        result = list(filter(str.strip, result))
        return result

    @property
    def is_ok(self):
        return all(x[0] for x in self.lines)


def match(pattern, line):
    pattern = pattern.replace('\t', ' '*8)
    line = line.replace('\t', ' '*8)
    pattern = re.escape(pattern)
    pattern = pattern.replace('\.\.\.', '.+?')
    pattern = re.compile('^'+pattern+'$')
    return pattern.match(line)


class Ellipsis(object):

    # other = other.replace('\t', ' '*8) oder allgemein white-space unsensibel
    # multi-line support

    def __init__(self, ellipsis):
        self.patterns = ellipsis.split('\n')

    def compare(self, lines):
        report = Report()

        patterns = self.patterns[:]
        # Keep track of whether we're on a multi-line ellipsis.
        multiline = False
        pattern = None

        for line in lines.split('\n'):
            # Select next applicable pattern.
            if multiline:
                report.matched(line)
                if pattern and match(pattern, line):
                    multiline = False
            else:
                if patterns:
                    pattern = patterns.pop(0)
                    if pattern == '...':
                        multiline = True
                        if patterns:
                            pattern = patterns.pop(0)
                        else:
                            pattern = None
                        report.matched(line)
                        continue
                    if match(pattern, line):
                        report.matched(line)
                    else:
                        report.nonmatched(line, pattern)
                else:
                    report.nonmatched(line, None)

        if multiline and pattern:
            # Get the unmatched multi-line boundary pattern back
            # into the list of unconsumed patterns.
            patterns.insert(0, pattern)

        for pattern in patterns:
            report.nonmatched(None, pattern)

        return report

    def __eq__(self, other):
        assert isinstance(other, str)
        report = self.compare(other)
        return report.is_ok
