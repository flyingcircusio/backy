# -*- encoding: utf-8 -*-

from fadvise import posix_fadvise, POSIX_FADV_SEQUENTIAL, POSIX_FADV_DONTNEED
import datetime
import glob
import gzip
import hashlib
import optparse
import os
import shutil
import sys
import threading
import time
import traceback
import backy


running = True


def main():
    if options.backup or options.restore or options.scrub:
        Stats() # init

        def show_stats():
            global running
            if running:
                print Stats()
                threading.Timer(1.0, show_stats).start()
        threading.Timer(1.0, show_stats).start()

    if options.clean:
        clean(args, options)
    elif options.backup:
        backup(args, options)
    elif options.restore:
        easy_restore(args, options)
    elif options.scrub:
        scrub(args, options)
    elif options.ls:
        ls(args, options)

    if options.backup or options.restore:
        print Stats()

    if options.backup or options.restore or options.clean:
        print "Done."

def main():

    try:
        start_time = time.time()
        parser = optparse.OptionParser(formatter=optparse.TitledHelpFormatter(), usage=globals()['__doc__'], version="1.0")
        parser.add_option ('-v', '--verbose', action='store_true', default=False, help='verbose output')
        parser.add_option ('-b', '--backup', action='store_true', default=False, help='backup')
        parser.add_option ('-r', '--restore', action='store_true', default=False, help='restore')
        parser.add_option ('-s', '--scrub', action='store_true', default=False, help='scrub')
        parser.add_option ('-c', '--clean', action='store_true', default=False, help='delete old diffs')
        parser.add_option ('-l', '--ls', action='store_true', default=False, help='list contents of backup')
        parser.add_option ('-f', '--force', action='store_true', default=False, help='force action')
        parser.add_option ('-n', '--checkonly', action='store_true', default=False, help='check only')
        (options, args) = parser.parse_args()

        tasks = options.backup + options.ls + options.restore + options.clean + options.scrub
        if tasks != 1:
            parser.error('You must supply exactly one of [-l, -r, -b, -c, -s].')
        if options.backup and len(args) < 2:
            parser.error('missing argument')
        if options.clean and len(args) < 2:
            parser.error('missing argument')
        if options.scrub and len(args) < 1:
            parser.error('missing argument')
        if options.ls and len(args) < 1:
            parser.error('missing argument')
        #if options.verbose: print time.asctime()
        main()
        if options.verbose:
            #print time.asctime()
            print 'TOTAL TIME: %.2f' % (time.time() - start_time)
        sys.exit(0)
    except KeyboardInterrupt, e: # Ctrl-C
        running = False
        raise e
    except SystemExit, e: # sys.exit()
        running = False
        raise e
    except Exception, e:
        running = False
        print 'ERROR, UNEXPECTED EXCEPTION'
        print str(e)
        traceback.print_exc()
        os._exit(1)

