""" Manage read-only volumes in local btrfs file system.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

from __future__ import division

import btrfs
import Butter
import Store

import datetime
import logging
import math
import os
import os.path

logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')
theMinimumChangeRate = .00001


class _Writer:

    """ Context Manager to write a snapshot. """

    def __init__(self, stream, path):
        self.stream = stream
        self.path = path

    def __enter__(self):
        return self.stream.__enter__()

    def __exit__(self, exceptionType, exception, trace):
        self.stream.__exit__(exceptionType, exception, trace)

        if exception is None:
            return

        if not os.path.exists(self.path):
            return

        partial = self.path + ".part"

        if os.path.exists(partial):
            partial = self.path + "_" + datetime.datetime.now().isoformat() + ".part"

        os.rename(self.path, partial)


class ButterStore(Store.Store):

    """ A local btrfs synchronization source or sink. """

    def __init__(self, host, path, isDest):
        """ Initialize.

        host is ignored.
        path is the file system location of the read-only subvolumes.

        """
        super(ButterStore, self).__init__()

        self.isDest = isDest
        self.path = os.path.abspath(path)
        logger.debug("%s", self.path)

        mountPath = self.path
        # User may have specified a destination subvolume, get the directory
        if not os.path.exists(mountPath):
            mountPath = os.path.dirname(mountPath)

        self.mount = mountPath
        self.butter = Butter.Butter(mountPath)  # subprocess command-line interface
        self.btrfs = btrfs.FileSystem(mountPath)     # ioctl interface

        self.butterVolumes = {}   # Dict of {uuid: <btrfs.Volume>}
        # self.volumes = self.butter.listVolumes()
        self._fillVolumesAndPaths()

    def _btrfsVol2StoreVol(self, bvol):
        uuid = bvol.received_uuid if self.isDest else bvol.uuid
        if uuid is None:
            return None
        return Store.Volume(uuid, bvol.totalSize, bvol.exclusiveSize, bvol.current_gen)

    def _fillVolumesAndPaths(self):
        """ Fill in self.paths. """
        with self.btrfs as mount:
            for bv in mount.subvolumes:
                if not bv.readOnly:
                    continue

                vol = self._btrfsVol2StoreVol(bv)
                if vol is None:
                    continue

                if vol.uuid in self.butterVolumes:
                    logger.warn("Duplicate effective uuid %s in '%s' and '%s'",
                        vol.uuid, bv.fullPath, self.butterVolumes[vol.uuid].fullPath)
                self.butterVolumes[vol.uuid] = bv

                # vol = Store.Volume(uuid, bv.totalSize, bv.exclusiveSize, bv.gen)
                for path in bv.linuxPaths:
                    if path.startswith(self.path):
                        path = path[len(self.path) + 1:]
                    logger.debug("%s %s", vol, path)
                    self.paths[vol].add(path)

    def __unicode__(self):
        """ English description of self. """
        return u"btrfs %s" % (self.path)

    def __str__(self):
        """ English description of self. """
        return unicode(self).encode('utf-8')

    def getEdges(self, fromVol):
        """ Return the edges available from fromVol. """
        if fromVol is None:
            for toVol in self.paths:
                yield Store.Diff(self, toVol, fromVol, toVol.size)
            return

        if fromVol not in self.paths:
            return

        fromBVol = self.butterVolumes[fromVol.uuid]
        parentUUID = fromBVol.parent_uuid

        vols = [vol for vol in self.butterVolumes.values()
                if vol.parent_uuid == parentUUID
                ]

        changeRate = self._calcChangeRate(vols)

        for toBVol in vols:
            if toBVol == fromBVol:
                continue

            # This gives a conservative estimate of the size of the diff

            estimatedSize = self._estimateSize(toBVol, fromBVol, changeRate)

            toVol = self._btrfsVol2StoreVol(toBVol)

            yield Store.Diff(self, toVol, fromVol, estimatedSize, True)

    def hasEdge(self, diff):
        """ True if Store already contains this edge. """
        return diff.toUUID in self.butterVolumes and diff.fromUUID in self.butterVolumes

    def receive(self, diff, paths):
        """ Return Context Manager for a file-like (stream) object to store a diff. """
        path = self.selectReceivePath(paths)
        path = os.path.normpath(os.path.join(os.path.dirname(self.path), path))
        logger.debug("Receiving '%s'", path)
        return _Writer(self.butter.receive(), path)

    def receiveVolumeInfo(self, paths):
        """ Return Context Manager for a file-like (stream) object to store volume info. """
        path = self.selectReceivePath(paths)
        path = os.path.normpath(os.path.join(self.path, path))
        return open(path + ".bs", "w")

    def _estimateSize(self, toBVol, fromBVol, changeRate):
        fromGen = fromBVol.current_gen
        genDiff = abs(toBVol.current_gen - fromGen)

        estimatedSize = max(0, toBVol.totalSize - fromBVol.totalSize)
        estimatedSize += toBVol.totalSize * (1 - math.exp(-changeRate * genDiff))
        estimatedSize = max(toBVol.exclusiveSize, estimatedSize)

        return estimatedSize

    def _calcChangeRate(self, bvols):
        total = 0
        diffs = 0
        minGen = bvols[0].current_gen
        maxGen = minGen
        minSize = bvols[0].totalSize
        maxSize = minSize

        for vol in bvols:
            total += vol.totalSize
            diffs += vol.exclusiveSize
            minGen = min(minGen, vol.current_gen)
            maxGen = max(maxGen, vol.current_gen)
            minSize = min(minSize, vol.totalSize)
            maxSize = max(maxSize, vol.totalSize)

        try:
            # exclusiveSize is often useless,
            # because data may be shared with read-write volumes not usable for send operations
            diffs = max(diffs, maxSize - minSize)
            rate = - math.log(1 - diffs / total) * (len(bvols) - 1) / (maxGen - minGen)
            rate /= 10  # Fudge
        except (ZeroDivisionError, ValueError):
            logger.debug("Using minimum change rate.")
            rate = theMinimumChangeRate

        logger.debug("Change rate: %f", rate)

        return rate

    def send(self, diff, streamContext, progress=True):
        """ Write the diff (toVol from fromVol) to the stream context manager. """
        self.butter.send(diff.toUUID, diff.fromUUID, streamContext, progress)
