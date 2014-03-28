
def scrub(args, options):
    backupfile = args.pop(0)
    if len(args) > 0:
        rollfile = args.pop()
    else:
        rollfile = "%s.roll" % backupfile
    reader = Reader(backupfile)
    roller = Rollfile(rollfile, backupfile)

    print "Starting scrub (%s vs. %s)" % (backupfile, rollfile),
    if options.checkonly:
        print "readonly."
    else:
        print "and marking bad chunks"

    i = 0
    changed = False
    while True:
        data = reader.getChunk(i)
        if not data:
            break
        if not roller.chunkMatches(i, data):
            roller.setChunk(i, '0')  # pretend a single 0 which should never match
                                     # in order to show that this chunk is destroyed
            changed = True
            if not options.checkonly:
                roller.write()
            print "Chunk %06d was destroyed." % i
        i += 1
    if changed and not options.checkonly:
        roller.write()
        pass
    reader.close()

