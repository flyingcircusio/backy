import backy.backup


def restore(backupdir, target, revision):
    backup = backy.backup.Backup(backupdir)
    if revision == 'last':
        revision = backup.revision_history[-1].uuid
    else:
        try:
            revision = int(revision)
            revision = backup.revision_history[-revision-1]
        except ValueError:
            revision = backup.revisions[revision]

    print "Restoring revision {}".format(revision.uuid)
    revision.restore(target)
