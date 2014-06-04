#! /usr/bin/python

import sys, os, os.path, subprocess, getopt

import file_sink
import s3_sink
import ssh_sink
import best_diffs

import re
import logging
import pprint

FORMAT = '%(levelname)7s:%(filename)s[%(lineno)d] %(funcName)s(): %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

options = [
    { 'short': "h", 'long': "help",         'default': False, 'description': "Display this help" },
    { 'short': "n", 'long': "dry-run",      'default': False, 'description': "Just print what it would do" },
    { 'short': "d", 'long': "delete",       'default': False, 'description': "Delete any snapshots in <dest> that are not in <src>" },
    { 'short': "r", 'long': "receive",      'default': False, 'description': "Internal command to intelligently receive diffs" },
    { 'short': "b", 'long': "batch",        'default': False, 'description': "Non-interactive" },
    { 'short': "q", 'long': "quiet",        'default': False, 'description': "Only error messages" },
]

usage = """
    butter_sync.py [options...] <src> <dest>

    <src>, <dest>:  file://path/to/directory
                    ssh://[user@]host/path/to/directory
                    s3://bucket/prefix
"""

optionFile = "~/butter_sync.conf"

def parseOptions(optionSpecs, args):
    longs = [ opt['long'] for opt in optionSpecs ]
    shorts = ''.join([ opt['short'] for opt in optionSpecs ])
    (userOptions, args) = getopt.getopt(args, shorts, longs)
    userOptions = dict(userOptions)

    for opt in options:
        if opt['long'] in userOptions:
            continue
        if opt['short'] in userOptions:
            userOptions[opt['long']] = userOptions[opt['short']]
            del userOptions[opt['short']]
        else:
            userOptions[opt['long']] = opt['default']

    logging.debug(userOptions)
    logging.debug(args)
    return (userOptions, args)

def parseSink(uri):
    # logging.debug(uri)
    pattern = re.compile('^(?P<method>[^:/]*)://(?P<host>[^/]*)(/(?P<path>.*))?$')
    match = pattern.match(uri)
    if match is None:
        return None
    parts = match.groupdict()

    if parts['method'] == 'file':
        parts['path'] = parts['host'] + '/' + parts['path']
    logging.debug(parts)

    Sinks = {
        'file': file_sink.FileSink,
        's3': s3_sink.S3Sink,
        # 'ssh': ssh_sink.ssh_sink,
    }

    return Sinks[parts['method']](parts['host'], parts['path'])

def main(argv=sys.argv):
    (opts, args) = parseOptions(options, argv[1:])

    if opts['help'] or len(args) < 1:
        print(usage)
        print(options)
        return 0
    
    source = parseSink(args[0])

    vols = source.listVolumes()

    if len(args) < 2:
        pprint.pprint(vols)
        return 0

    dest = parseSink(args[1])

    best = best_diffs.BestDiffs([vol['uuid'] for vol in vols])
    best.analyze(source, dest)

    pprint.pprint([ d for d in best.listDiffs() ])

    return 0

if __name__ == "__main__":
    sys.exit(main())
