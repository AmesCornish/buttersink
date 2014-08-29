#! /usr/bin/python

""" Main program to synchronize btrfs snapshots.  See README.md.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

if True:  # Headers
    if True:  # imports

        import argparse
        import errno
        import logging
        import os.path
        import re
        import sys

        import BestDiffs
        import ButterStore
        import S3Store
        import Store

theDebug = False

logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')

theVersionFile = os.path.join(os.path.dirname(__file__), "version.txt")
try:
    with open(theVersionFile) as version:
        theVersion = version.readline()
except IOError:
    print("Missing '%s'" % (theVersionFile))
    theVersion = "<unknown>"

theChunkSize = 100

command = argparse.ArgumentParser(
    description="Synchronize two sets of btrfs snapshots.",
    epilog="""
<src>, <dst>:   [btrfs://]/path/to/directory/[snapshot]
                s3://bucket/prefix/[snapshot]

If only <dst> is supplied, just list available snapshots.  NOTE: The trailing
"/" *is* significant.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.
See README.md and LICENSE.txt for more info.
    """,
    formatter_class=argparse.RawDescriptionHelpFormatter,
)

command.add_argument('source', metavar='<src>', nargs='?',  # nargs='+',
                     help='a source of btrfs snapshots')
command.add_argument('dest', metavar='<dst>',
                     help='the btrfs snapshots to be updated')

command.add_argument('-n', '--dry-run', action="store_true",
                     help="display what would be transferred, but don't do it",
                     )
command.add_argument('-d', '--delete', action="store_true",
                     help='delete any snapshots in <dst> that are not in <src>',
                     )

command.add_argument('-q', '--quiet', action="count", default=0,
                     help="""
                     once: don't display progress.
                     twice: only display error messages""",
                     )
command.add_argument('-l', '--logfile', type=argparse.FileType('a'),
                     help='log debugging information to file',
                     )
command.add_argument('-V', '--version', action="version", version='%(prog)s ' + theVersion,
                     help='display version',
                     )

command.add_argument('--part-size', action="store", type=int, default=theChunkSize,
                     help='Size of chunks in a multipart upload',
                     )

command.add_argument('--remote-receive', action="store_true",
                     help=argparse.SUPPRESS,
                     )
command.add_argument('--remote-send', action="store_true",
                     help=argparse.SUPPRESS,
                     )
command.add_argument('--remote-list', action="store_true",
                     help=argparse.SUPPRESS,
                     )


def _setupLogging(quietLevel, logFile):
    theDisplayFormat = '%(message)s'
    theDebugDisplayFormat = (
        '%(levelname)7s:'
        '%(filename)s[%(lineno)d] %(funcName)s(): %(message)s'
    )
    theLogFormat = (
        '%(asctime)-15s: %(levelname)7s:'
        '%(filename)s[%(lineno)d] %(funcName)s(): %(message)s'
    )

    root = logging.getLogger()
    root.setLevel("INFO" if theDebug else "DEBUG")  # When debugging, this is handled per-logger

    def add(handler, level, format):
        handler = logging.StreamHandler(handler)
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter(format))
        root.addHandler(handler)

    level = "DEBUG" if theDebug else "INFO" if quietLevel < 2 else "WARN"
    formatString = theDebugDisplayFormat if theDebug else theDisplayFormat

    add(sys.stdout, level, formatString)

    if logFile is not None:
        add(logFile, "DEBUG", theLogFormat)

    logging.getLogger('boto').setLevel("WARN")


def parseSink(uri, isDest, dryrun):
    """ Parse command-line description of sink into a sink object. """
    if uri is None:
        return None

    # logger.debug(uri)
    pattern = re.compile('^((?P<method>[^:/]*)://)?(?P<host>[^/]*)(/(?P<path>.*))?$')
    match = pattern.match(uri)
    if match is None:
        # logger.error("Can't parse snapshot store '%s'", uri)
        raise Exception("Can't parse snapshot store '%s'" % (uri))
    parts = match.groupdict()

    if parts['method'] is None:
        parts['method'] = 'btrfs'

    if parts['method'] in ('btrfs', 'file'):
        parts['path'] = parts['host'] + '/' + parts['path']

    logger.debug(parts)

    Sinks = {
        'btrfs': ButterStore.ButterStore,
        # 'file': FileStore,
        's3': S3Store.S3Store,
        # 'ssh': SSHStore.SSHStore,
    }

    return Sinks[parts['method']](parts['host'], parts['path'], isDest, dryrun)


def main():
    """ Main program. """
    try:
        args = command.parse_args()

        _setupLogging(args.quiet, args.logfile)

        logger.debug("Arguments: %s", vars(args))

        progress = args.quiet == 0

        source = parseSink(args.source, False, args.dry_run)

        dest = parseSink(args.dest, source is not None, args.dry_run)

        if source is None:
            source = dest
            dest = None

        try:
            next(source.listVolumes())
        except StopIteration:
            logger.error("No snapshots in source.")
            path = args.source or args.dest
            if not path.endswith("/"):
                logger.error("Try adding a '/' to '%s'.", path)
            return 1

        if dest is None:
            for item in source.listContents():
                print item
            if args.delete:
                source.deletePartials()
            return 0

        best = BestDiffs.BestDiffs(source.listVolumes(), args.delete)
        best.analyze(source, dest)

        summary = best.summary()
        logger.info("Optimal synchronization:")
        for sink, values in summary.items():
            logger.info("%s from %d diffs in %s",
                        Store.humanize(values.size),
                        values.count,
                        sink or "TOTAL",
                        )

        for diff in best.iterDiffs():
            if diff is None:
                raise Exception("Missing diff.  Can't fully replicate.")
            else:
                diff.sendTo(dest, chunkSize=args.part_size << 20, progress=progress)

        if args.delete:
            dest.deleteUnused()

        logger.debug("Successful exit")

        return 0
    except Exception as error:
        if (
            isinstance(error, IOError) and
            error.errno == errno.EPERM and
            os.getuid() != 0
        ):
            logger.error("You must be root to access a btrfs filesystem.  Use  'sudo'")
        else:
            logger.exception("")
        return 1

if __name__ == "__main__":
    sys.exit(main())
