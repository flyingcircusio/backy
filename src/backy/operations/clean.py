import backy.backup


def clean(target, keep):
    backup = backy.backup.Backup(target)

    revisions = backup.revisions.values()
    revisions.sort(key=lambda r: r.timestamp)
    for r in revisions[:-keep]:
        print "Removing revision {}".format(r.uuid)
        r.remove()
