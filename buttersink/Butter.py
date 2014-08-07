""" Interface to btrfs-tools for snapshots.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

if True:  # Headers
    if True:  # imports
        import os
        import os.path
        import psutil
        import re
        import subprocess
        import sys

        import Store

    if True:  # constants
        import logging
        logger = logging.getLogger(__name__)

        theChunkSize = 100 * (2 ** 20)

        DEVNULL = open(os.devnull, 'wb')

# logger.setLevel('DEBUG')


class Butter:

    """ Interface to local btrfs file system snapshots. """

    def __init__(self, dryrun):
        """ Initialize. """
        self.btrfsVersion = self._getVersion([3, 14])
        self.dryrun = dryrun

    def _getVersion(self, minVersion):
        btrfsVersionString = subprocess.check_output(
            ["btrfs", "--version"], stderr=sys.stderr
        ).decode("utf-8").strip()

        versionPattern = re.compile("[0-9]+(\.[0-9]+)*")
        version = versionPattern.search(btrfsVersionString)

        try:
            version = [int(num) for num in version.group(0).split(".")]
        except AttributeError:
            version = None

        if version < [3, 14]:
            logger.error(
                "%s is not supported.  Please upgrade your btrfs to at least 3.14",
                btrfsVersionString
            )
        else:
            logger.debug("%s", btrfsVersionString)

        return btrfsVersionString

    def processReceive(self, directory):
        """ Return a process that will store a diff. """
        cmd = ["btrfs", "receive", directory]

        if Store.skipDryRun(logger, self.dryrun)("Command: %s", cmd):
            return None

        process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=sys.stderr, stdout=DEVNULL)

        ps = psutil.Process(process.pid)
        ps.ionice(psutil.IOPRIO_CLASS_IDLE)

        return process

    def send(self, targetPath, parent, streamContext, progress=True):
        """ Write a (incremental) snapshot to the stream context manager. """
        if parent is not None:
            cmd = ["btrfs", "send", "-p", parent, targetPath]
        else:
            cmd = ["btrfs", "send", targetPath]

        if Store.skipDryRun(logger, self.dryrun)("Command: %s", cmd):
            return

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr)
        ps = psutil.Process(process.pid)
        ps.ionice(psutil.IOPRIO_CLASS_IDLE)

        try:
            streamContext.metadata['btrfsVersion'] = self.btrfsVersion
        except AttributeError:
            pass

        try:
            streamContext.progress = progress
        except AttributeError:
            pass

        with streamContext as stream:
            while True:
                data = process.stdout.read(theChunkSize)
                if len(data) == 0:
                    break
                stream.write(data)
