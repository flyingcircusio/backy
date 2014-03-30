import backy.backup


def backup(source, target):
    backup = backy.backup.Backup(target)
    backup.backup(source)
