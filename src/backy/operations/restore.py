import backy.backup


def restore(backupdir, target, revision):
    backup = backy.backup.Backup(backupdir)
    if revision == 'last':
        revision = backup.revision_history[-1].uuid
    r = backup.revisions[revision]
    print "Restoring revision {}".format(r.uuid)
    r.restore(target)
