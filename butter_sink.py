#! /usr/bin/python

""" Main program to synchronize btrfs snapshots.  See ReadMe.md. """

import argparse
import logging
import pprint
import re
import sys

# import ssh_sink
import best_diffs
import file_sink
import s3_sink

theLogFormat = '%(levelname)7s:%(filename)s[%(lineno)d] %(funcName)s(): %(message)s'
logging.basicConfig(level='INFO', format=theLogFormat)
logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')

command = argparse.ArgumentParser(
    description="Synchronize two sets of btrfs snapshots.",
    epilog="""
<src>, <dst>:   file://path/to/directory
                ssh://[user@]host/path/to/directory
                s3://bucket/prefix

If only <dst> is supplied, just list available snapshots.
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
command.add_argument('-r', '--receive', action="store_true",
                     help='internal command to intelligently receive diffs',
                     )
command.add_argument('-b', '--batch', action="store_true",
                     help='non-interactive',
                     )
command.add_argument('-q', '--quiet', action="store_true",
                     help='only display error messages',
                     )

optionFile = "~/butter_sync.conf"


def parseSink(uri):
    """ Parse command-line description of sink into a sink object. """
    # logger.debug(uri)
    pattern = re.compile('^(?P<method>[^:/]*)://(?P<host>[^/]*)(/(?P<path>.*))?$')
    match = pattern.match(uri)
    if match is None:
        return None
    parts = match.groupdict()

    if parts['method'] == 'file':
        parts['path'] = parts['host'] + '/' + parts['path']
    logger.debug(parts)

    Sinks = {
        'file': file_sink.FileSink,
        's3': s3_sink.S3Sink,
        # 'ssh': ssh_sink.ssh_sink,
    }

    return Sinks[parts['method']](parts['host'], parts['path'])


def main(argv=sys.argv):
    """ Main program. """
    args = command.parse_args()
    logger.debug("Arguments: %s", vars(args))

    source = parseSink(args.source)

    vols = source.listVolumes()

    dest = parseSink(args.dest)

    if dest is None:
        pprint.pprint(vols)
        return 0

    best = best_diffs.BestDiffs([vol['uuid'] for vol in vols], args.delete)
    best.analyze(source, dest)

    summary = best.summary()
    logger.info("Optimal synchronization: %d diffs, %f MB total", summary["count"], summary["size"])
    for sink, size in summary["sinks"].items():
        logger.info("%f MB from %s", size, sink)

    for diff in best.iterDiffs():
        logger.info("%s", diff)
        if not args.dry_run:
            dest.receive(diff)

    return 0

if __name__ == "__main__":
    sys.exit(main())
