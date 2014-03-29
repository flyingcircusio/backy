from backy.format import Reader, Rollfile


def scrub(target, checkonly=False):
    backupfile = target
    reader = Reader(backupfile)
    roller = Rollfile(backupfile)

    print "Starting scrub on %s" % backupfile
    if checkonly:
        print "readonly."
    else:
        print "and marking bad chunks"

    for i, data in reader.iterchunks():
        if roller.chunkMatches(i, data):
            continue
        # pretend a single 0 which should never match in order to show that
        # this chunk is destroyed
        roller.setChunk(i, '0')
        if not checkonly:
            print "Chunk %06d is corrupt and has been marked." % i
            roller.write()
        else:
            print "Chunk %06d is corrupt." % i

    reader.close()
