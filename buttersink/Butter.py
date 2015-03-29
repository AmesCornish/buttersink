""" Interface to btrfs-tools for snapshots.

Copyright (c) 2014-2015 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

if True:  # Headers
    if True:  # imports
        import datetime
        import io
        import os
        import os.path
        import psutil
        import re
        import subprocess
        import sys

        from progress import DisplayProgress
        import btrfs
        import send
        import Store

    if True:  # constants
        import logging
        logger = logging.getLogger(__name__)

        DEVNULL = open(os.devnull, 'wb')


# logger.setLevel('DEBUG')

# Btrfs should copy the sending received UUID to the receving UUID,
# but instead it copies the sending current UUID to the receving UUID.
# This prevents diff parents from being properly identified.
# This can be fixed by changing the UUID and transid's duing send/receive.
FIXUP_AFTER_RECEIVE = False
FIXUP_DURING_SEND = True
FIXUP_DURING_RECEIVE = True


def _makeNice(process):
    try:
        ps = psutil.Process(process.pid)
        ps.ionice(psutil.IOPRIO_CLASS_IDLE)
    except AttributeError:
        logger.debug("ionice is not available")


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

    def receive(self, path, diff, showProgress=True):
        """ Return a context manager for stream that will store a diff. """
        directory = os.path.dirname(path)

        cmd = ["btrfs", "receive", "-e", directory]

        if Store.skipDryRun(logger, self.dryrun)("Command: %s", cmd):
            return None

        if not os.path.exists(directory):
            os.makedirs(directory)

        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdout=DEVNULL,
        )
        _makeNice(process)

        return _Writer(process, process.stdin, path, diff, showProgress)

    def send(self, targetPath, parent, diff, showProgress=True, allowDryRun=True):
        """ Return context manager for stream to send a (incremental) snapshot. """
        if parent is not None:
            cmd = ["btrfs", "send", "-p", parent, targetPath]
        else:
            cmd = ["btrfs", "send", targetPath]

        if Store.skipDryRun(logger, self.dryrun and allowDryRun)("Command: %s", cmd):
            return None

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=DEVNULL)
        _makeNice(process)

        return _Reader(process, process.stdout, targetPath, diff, showProgress)


class _Writer(io.RawIOBase):

    """ Context Manager to write a snapshot. """

    def __init__(self, process, stream, path, diff, showProgress):
        self.process = process
        self.stream = stream
        self.path = path
        self.diff = diff
        self.bytesWritten = None
        self.progress = DisplayProgress(diff.size) if showProgress else None

    def __enter__(self):
        self.bytesWritten = 0
        if self.progress is not None:
            self.progress.open()
        return self

    def __exit__(self, exceptionType, exception, trace):
        self.stream.close()

        if self.progress is not None:
            self.progress.close()

        if self.process is None:
            return

        logger.debug("Waiting for receive process to finish...")
        self.process.wait()

        if exception is None and self.process.returncode == 0:
            # Fixup with SET_RECEIVED_SUBVOL
            if FIXUP_AFTER_RECEIVE:
                received = btrfs.SnapShot(self.path)
                received.SET_RECEIVED_SUBVOL(
                    uuid=self.diff.toUUID,
                    stransid=self.diff.toGen,
                )
            return

        if self.process.returncode != 0:
            logger.error("btrfs receive errors")
            for line in self.process.stderr:
                sys.stderr.write(line)

        if os.path.exists(self.path):
            # This tries to mark partial (failed) transfers.

            partial = self.path + ".part"

            if os.path.exists(partial):
                partial = self.path + "_" + datetime.datetime.now().isoformat() + ".part"

            os.rename(self.path, partial)
            logger.debug("Renamed %s to %s", self.path, partial)

        if exception is None:
            raise Exception(
                "receive %s returned error %d."
                % (self.path, self.process.returncode, )
            )

    def write(self, data):
        # If it's the first big chunk (header)
        # Tweak the volume information to match what we expect.
        if FIXUP_DURING_RECEIVE and self.bytesWritten == 0:
            data = send.replaceIDs(
                data,
                self.diff.toUUID,
                self.diff.toGen,
                self.diff.fromUUID,
                self.diff.fromGen,
            )
        self.stream.write(data)
        self.bytesWritten += len(data)
        if self.progress is not None:
            self.progress.update(self.bytesWritten)


class _Reader(io.RawIOBase):

    """ Context Manager to read a snapshot. """

    def __init__(self, process, stream, path, diff, showProgress):
        self.process = process
        self.stream = stream
        self.path = path
        self.diff = diff
        self.bytesRead = None
        self.progress = DisplayProgress() if showProgress else None

    def __enter__(self):
        self.bytesRead = 0
        if self.progress is not None:
            self.progress.open()
        return self

    def __exit__(self, exceptionType, exception, trace):
        self.stream.close()

        if self.progress is not None:
            self.progress.close()

        if self.process is None:
            return

        logger.debug("Waiting for send process to finish...")
        self.process.wait()

        if self.process.returncode != 0:
            logger.error("btrfs send errors")
            for line in self.process.stderr:
                sys.stderr.write(line)

        if exception is None and self.process.returncode != 0:
            raise Exception(
                "send returned error %d. %s may be corrupt."
                % (self.process.returncode, self.path)
            )

    def read(self, size):
        # If it's the first big chunk (header)
        # Tweak the volume information to match what we expect.
        data = self.stream.read(size)
        if FIXUP_DURING_SEND and self.bytesRead == 0:
            data = send.replaceIDs(
                data,
                self.diff.toUUID,
                self.diff.toGen,
                self.diff.fromUUID,
                self.diff.fromGen,
            )
        self.bytesRead += len(data)
        if self.progress is not None:
            self.progress.update(self.bytesRead)
        return data

    def seek(self, offset, whence):
        self.stream.seek(offset, offset, whence)
        if whence == io.SEEK_SET:
            self.bytesRead = offset
        elif whence == io.SEEK_CUR:
            pass
        elif whence == io.SEEK_END:
            self.bytesRead = None
