import backy


def scrub(target, revision, markbad=True):
    backup = backy.backup.Backup(target)
    r = backup.revisions[revision]
    r.scrub(markbad)
