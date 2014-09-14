""" Interface to btrfs-tools for snapshots.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

if True:  # Headers
    if True:  # imports
        import datetime
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

    def receive(self, path):
        """ Return a context manager for stream that will store a diff. """
        directory = os.path.dirname(path)

        cmd = ["btrfs", "receive", directory]

        if Store.skipDryRun(logger, self.dryrun)("Command: %s", cmd):
            return None

        process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=sys.stderr, stdout=DEVNULL)
        ps = psutil.Process(process.pid)
        ps.ionice(psutil.IOPRIO_CLASS_IDLE)

        return _Writer(process, path)

    def send(self, targetPath, parent):
        """ Return context manager for stream to send a (incremental) snapshot. """
        if parent is not None:
            cmd = ["btrfs", "send", "-p", parent, targetPath]
        else:
            cmd = ["btrfs", "send", targetPath]

        if Store.skipDryRun(logger, self.dryrun)("Command: %s", cmd):
            return None

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=DEVNULL)
        ps = psutil.Process(process.pid)
        ps.ionice(psutil.IOPRIO_CLASS_IDLE)

        return _Reader(process, targetPath)


class _Writer:

    """ Context Manager to write a snapshot. """

    def __init__(self, process, path):
        self.process = process
        self.stream = process.stdin
        self.path = path

    def __enter__(self):
        return self.stream.__enter__()

    def __exit__(self, exceptionType, exception, trace):
        self.stream.__exit__(exceptionType, exception, trace)

        logger.debug("Waiting for receive process to finish...")
        self.process.wait()

        if exception is None and self.process.returncode == 0:
            return

        if os.path.exists(self.path):
            # This tries to mark partial (failed) transfers.

            partial = self.path + ".part"

            if os.path.exists(partial):
                partial = self.path + "_" + datetime.datetime.now().isoformat() + ".part"

            os.rename(self.path, partial)

        if exception is None:
            raise Exception(
                "receive returned error %d. %s may be corrupt."
                % (self.process.returncode, self.path)
                )


class _Reader:

    """ Context Manager to read a snapshot. """

    def __init__(self, process, path):
        self.process = process
        self.stream = process.stdout
        self.path = path

    def __enter__(self):
        return self.stream.__enter__()

    def __exit__(self, exceptionType, exception, trace):
        self.stream.__exit__(exceptionType, exception, trace)

        logger.debug("Waiting for send process to finish...")
        self.process.wait()

        if exception is None and self.process.returncode == 0:
            return

        if exception is None:
            raise Exception(
                "send returned error %d. %s may be corrupt."
                % (self.process.returncode, self.path)
                )
