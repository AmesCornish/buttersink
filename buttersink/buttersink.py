#! /usr/bin/python

""" Main program to synchronize btrfs snapshots.  See README.md.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.
"""

if True:  # Headers
    if True:  # imports

        import argparse
        import logging
        import re
        import sys

        import BestDiffs
        import ButterStore
        import S3Store
        import Store

theVersion = '0.2+'
theDebug = True

logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')

command = argparse.ArgumentParser(
    description="Synchronize two sets of btrfs snapshots.",
    epilog="""
<src>, <dst>:   file:///path/to/directory
                ssh://[user@]host/path/to/directory (Not implemented)
                s3://bucket/prefix[/snapshot]

If only <dst> is supplied, just list available snapshots.

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
command.add_argument('-l', '--logfile', type=argparse.FileType('w'),
                     help='log debugging information to file',
                     )
command.add_argument('-V', '--version', action="version", version='%(prog)s ' + theVersion,
                     help='display version',
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
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter(format))
        root.addHandler(handler)

    level = "DEBUG" if theDebug else "INFO" if quietLevel < 2 else "WARN"
    formatString = theDebugDisplayFormat if theDebug else theDisplayFormat

    add(logging.StreamHandler(sys.stdout), level, formatString)

    if logFile is not None:
        add(logging.StreamHandler(logFile), "DEBUG", theLogFormat)


def parseSink(uri, isDest):
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
        parts['method'] = 'file'

    if parts['method'] == 'file':
        parts['path'] = parts['host'] + '/' + parts['path']

    logger.debug(parts)

    Sinks = {
        'file': ButterStore.ButterStore,
        's3': S3Store.S3Store,
        # 'ssh': SSHStore.SSHStore,
    }

    return Sinks[parts['method']](parts['host'], parts['path'], isDest)


def main():
    """ Main program. """
    args = command.parse_args()

    _setupLogging(args.quiet, args.logfile)

    logger.debug("Arguments: %s", vars(args))

    progress = args.quiet == 0

    source = parseSink(args.source, False)

    dest = parseSink(args.dest, source is not None)

    if source is None:
        for vol in dest.listVolumes():
            print vol.display(dest, detail="line")
        return 0

    vols = source.listVolumes()

    best = BestDiffs.BestDiffs(vols, args.delete)
    best.analyze(source, dest)

    summary = best.summary()
    logger.info("Optimal synchronization: %d diffs, %s total",
                summary["count"], Store.humanize(summary["size"]))
    for sink, size in summary["sinks"].items():
        logger.info("%s from %s", Store.humanize(size), sink)

    for diff in best.iterDiffs():
        logger.info("%s: %s", "Keep" if diff.sink == dest else "Xfer", diff)

        if diff.sink == dest:
            continue

        vol = diff.toVol
        paths = diff.sink.getPaths(vol)

        streamContext = dest.receive(diff, paths, dryrun=args.dry_run)

        diff.sink.send(diff, streamContext, progress=progress, dryrun=args.dry_run)

        infoContext = dest.receiveVolumeInfo(paths, dryrun=args.dry_run)

        if args.dry_run:
            infoContext = sys.stdout

        vol.writeInfo(infoContext)

    return 0

if __name__ == "__main__":
    sys.exit(main())
