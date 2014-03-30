import backy


def scrub(target, revision, markbad=True):
    backup = backy.backup.Backup(target)

    if revision == 'all':
        revisions = backup.revisions.values()
    else:
        revisions = [backup.revisions[revision]]

    for r in revisions:
        print "Scrubbing {} ...".format(r.uuid)
        r.scrub(markbad)
