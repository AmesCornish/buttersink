""" Interface to btrfs-tools for snapshots. """

import subprocess
import os.path

import logging
logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')


class Butter:

    """ Interface to local btrfs file system snapshots. """

    def __init__(self, path):
        """ Initialize.

        path indicates the btrfs volume, and also the directory containing snapshots.
        """
        self.path = path

        self.mount = subprocess.check_output(
            ["df", path]
        ).decode("utf-8").rsplit(None, 1)[1]

        self.relPath = os.path.relpath(path, self.mount)

        self.id = int(subprocess.check_output(
            ["btrfs", "inspect", "rootid", self.mount]
        ).decode("utf-8"))

        self.volumes = self._getVolumes()

    def listVolumes(self):
        """ List all read-only volumes under path. """
        return self.volumes

    def _getVolumes(self, readOnly=True):
        vols = {}
        volsByID = {}

        result = subprocess.check_output(
            ["btrfs", "sub", "list", "-put", "-r" if readOnly else "", self.mount]
        ).decode("utf-8")

        for line in result.splitlines()[2:]:
            (id, gen, parent, top, uuid, path) = line.split()

            if int(top) != self.id:
                continue

            if not path.startswith(self.relPath):
                continue

            vol = {
                'id': int(id),
                'gen': int(gen),
                'parent': int(parent),
                # 'top': int(top),
                'uuid': uuid,
                'path': os.path.relpath(path, self.relPath),
            }

            vols[uuid] = vol
            volsByID[int(id)] = vol

        try:
            usage = subprocess.check_output(["btrfs", "qgroup", "show", self.mount])
        except subprocess.CalledProcessError:
            logger.warn("Rescanning subvolume sizes (this may take a while)...")
            subprocess.check_call(["btrfs", "quota", "enable", self.mount])
            subprocess.check_call(["btrfs", "quota", "rescan", "-w", self.mount])
            usage = subprocess.check_output(["btrfs", "qgroup", "show", self.mount])

        for line in usage.splitlines()[2:]:
            (qgroup, totalSize, exclusiveSize) = line.split()
            volID = int(qgroup.split("/")[-1])

            if volID in volsByID:
                volsByID[volID]['totalSize'] = float(totalSize) / 2 ** 20
                volsByID[volID]['exclusiveSize'] = float(exclusiveSize) / 2 ** 20

        return vols

    def send(self, uuid, parent):
        """ Send a (incremental) snapshot.

        Return the stream object that will send the data.
        """
        targetPath = os.path.join(self.path, self.volumes[uuid]['path'])

        if parent is not None:
            parentPath = os.path.join(self.path, self.volumes[parent]['path'])
            cmd = ["btrfs", "send", "-p", parentPath, targetPath]
        else:
            cmd = ["btrfs", "send", targetPath]

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=-1)
        return process.stdout
