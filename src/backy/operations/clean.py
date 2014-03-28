
def clean(args, options):
    infile = args.pop(0)
    level = int(args.pop(0))
    if len(args) > 0:
        prefix = args[0]
    else:
        prefix = "%s.diff" % infile
    num_keep = int(level)
    if num_keep < 1 and not options.force:
        raise ValueError("You must keep at least one diff.")

    diffs = sorted(glob.glob("%s.*" % prefix), reverse=True)
    diffs = [d for d in diffs if not d.endswith(".index") and not d.endswith(".md5")]

    delete = diffs[num_keep:]
    for d in delete:
        files = sorted(glob.glob("%s*" % d))
        for f in files:
            print "Deleting: %s" % f
            os.unlink(f)
