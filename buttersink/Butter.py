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

    if True:  # constants
        import logging
        logger = logging.getLogger(__name__)

        theChunkSize = 100 * (2 ** 20)

        DEVNULL = open(os.devnull, 'wb')

# logger.setLevel('DEBUG')


class Butter:

    """ Interface to local btrfs file system snapshots. """

    def __init__(self, path):
        """ Initialize.

        path indicates the btrfs volume, and also the directory containing snapshots.

        """
        self.btrfsVersion = self._getVersion([3, 14])

        if not os.path.isdir(path):
            raise Exception("'%s' is not an existing directory" % (path))

        # userPath - user specified mounted path to directory with snapshots
        self.userPath = os.path.abspath(path)

        # Get tree ID of the containing subvolume of path.
        self.mountID = int(subprocess.check_output(
            ["btrfs", "inspect", "rootid", self.userPath], stderr=sys.stderr
        ).decode("utf-8"))

        # mountPath - mounted path to mount point (doesn't always work!)
        self.mountPath = subprocess.check_output(
            ["df", path], stderr=sys.stderr
        ).decode("utf-8").rsplit(None, 1)[1]

        # relPath - relative path from mountPath to userPath
        self.relPath = os.path.relpath(path, self.mountPath)

        butterPath = subprocess.check_output(
            ["btrfs", "inspect", "subvol", str(self.mountID), self.mountPath], stderr=sys.stderr
        ).decode("utf-8").strip()

        # topPath - btrfs path to immediately enclosing subvolume
        self.topPath = "<FS_TREE>/" + butterPath

        # butterPath - btrfs path to user specified directory with snapshots
        self.butterPath = os.path.normpath(os.path.join(self.topPath, self.relPath))

        logger.debug(
            "MountID: %d, Mount: %s, Path: %s",
            self.mountID, self.mountPath, self.butterPath
        )

        self.volumes = self._getVolumes()

    def listVolumes(self):
        """ List all read-only volumes under path. """
        return self.volumes

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

    def _getVolumes(self, readOnly=True):
        vols = {}
        volsByID = {}

        logger.info('Listing "%s" snapshots...', self.relPath)

        self._fileSystemSync()

        result = subprocess.check_output(
            ["btrfs", "sub", "list", "-puta", "-r" if readOnly else "", self.mountPath],
            stderr=sys.stderr
        ).decode("utf-8")

        logger.debug("User path in btrfs: %s", self.butterPath)

        for line in result.splitlines()[2:]:
            logger.debug("%s", line)

            (id, gen, parent, top, uuid, path) = line.split()

            if not path.startswith("<FS_TREE>"):
                if int(top) != self.mountID:
                    logger.error("Can't find absolute path for snapshot %s (%s)", path, id)
                else:
                    path = os.path.normpath(os.path.join(os.path.dirname(self.topPath), path))

            extra = not path.startswith(self.butterPath)

            logger.debug("%s snapshot path in btrfs: %s", "Extra" if extra else "REQUIRED", path)

            if not extra:
                path = os.path.relpath(path, self.butterPath)

            vol = {
                'id': int(id),
                'gen': int(gen),
                'parent': int(parent),
                # 'top': int(top),
                'uuid': uuid,
                'path': path,
                'extra': extra,
            }

            vols[uuid] = vol
            volsByID[int(id)] = vol

        try:
            usage = subprocess.check_output(
                ["btrfs", "qgroup", "show", self.mountPath],
                stderr=sys.stderr)
        except subprocess.CalledProcessError:
            logger.warn("Rescanning subvolume sizes (this may take a while)...")
            subprocess.check_call(
                ["btrfs", "quota", "enable", self.mountPath],
                stderr=sys.stderr)
            subprocess.check_call(
                ["btrfs", "quota", "rescan", "-w", self.mountPath],
                stderr=sys.stderr)
            usage = subprocess.check_output(
                ["btrfs", "qgroup", "show", self.mountPath],
                stderr=sys.stderr)

        for line in usage.splitlines()[2:]:
            (qgroup, totalSize, exclusiveSize) = line.split()
            volID = int(qgroup.split("/")[-1])

            if volID in volsByID:
                logger.debug("Snap info: %s", line)
                volsByID[volID]['totalSize'] = int(totalSize)
                volsByID[volID]['exclusiveSize'] = int(exclusiveSize)

        return vols

    def receive(self, directory, dryrun=False):
        """ Return a file-like (stream) object to store a diff. """
        directory = os.path.normpah(os.path.join(self.userPath, directory))
        cmd = ["btrfs", "receive", directory]

        if dryrun:
            print("%s" % (" ".join(cmd)))
            return None

        self._fileSystemSync()
        process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=sys.stderr, stdout=DEVNULL)

        ps = psutil.Process(process.pid)
        ps.ionice(psutil.IOPRIO_CLASS_IDLE)

        return process.stdin

    def _getPath(self, uuid):
        path = self.volumes[uuid]['path']
        if path.startswith('<FS_TREE>'):
            return path[9:]
        else:
            return os.path.normpath(os.path.join(
                self.userPath,
                path
            ))

    def _fileSystemSync(self):
        subprocess.check_call(["sync"], stderr=sys.stderr)
        subprocess.check_call(
            ["btrfs", "filesystem", "sync", self.mountPath],
            stderr=sys.stderr)
        subprocess.check_call(["sync"], stderr=sys.stderr)

    def _linux2ButterPath(self, path):
        raise NotImplementedError

    def _butter2LinuxPath(self, path):
        raise NotImplementedError

    def send(self, uuid, parent, streamContext, progress=True, dryrun=False):
        """ Write a (incremental) snapshot to the stream context manager. """
        targetPath = self._getPath(uuid)

        if parent is not None:
            cmd = ["btrfs", "send", "-p", self._getPath(parent), targetPath]
        else:
            cmd = ["btrfs", "send", targetPath]

        if dryrun:
            print(" ".join(cmd))
            return

        logger.debug("Command: %s", cmd)

        self._fileSystemSync()

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
