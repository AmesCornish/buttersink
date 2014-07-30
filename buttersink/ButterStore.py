""" Manage read-only volumes in local btrfs file system.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

from __future__ import division

import btrfs
import Butter
import Store

import collections
import datetime
import logging
import math
import os
import os.path

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')
theMinimumChangeRate = .00001


class _Writer:

    """ Context Manager to write a snapshot. """

    def __init__(self, stream, volume, path):
        self.stream = stream
        self.volume = volume
        self.path = path

    def __enter__(self):
        return self.stream.__enter__()

    def __exit__(self, exceptionType, exception, trace):
        self.stream.__exit__(exceptionType, exception, trace)

        if exception is None:
            with open(self.path + ".bs") as stream:
                self.volume.writeInfo(stream)
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
        self.butter = Butter.Butter(self.path)  # subprocess command-line interface
        self.btrfs = btrfs.Mount(self.path)     # ioctl interface

        # self.volumes = self.butter.listVolumes()
        self._fillVolumesAndPaths()

    def _fillVolumesAndPaths(self):
        """ Fill in self.paths. """
        # import pudb; pudb.set_trace()
        with self.btrfs as mount:
            for bv in mount.subvolumes:
                if not bv.readOnly:
                    continue
                uuid = bv.ruuid if self.isDest else bv.uuid
                vol = Store.Volume(uuid, None, None, bv.current_gen)
                # vol = Store.Volume(uuid, bv.totalSize, bv.exclusiveSize, bv.gen)
                logger.debug("%s %s", vol, bv.fullPath)
                self.paths[vol].add(bv.fullPath)

    def __unicode__(self):
        """ English description of self. """
        return u"btrfs %s" % (self.path)

    def __str__(self):
        """ English description of self. """
        return unicode(self).encode('utf-8')

    def getEdges(self, fromVol):
        """ Return the edges available from fromVol.

        Returned edge is a dict: 'to' UUID, estimated 'size' in bytes

        """
        if fromVol is None:
            for toVol in self.volumes.values():
                yield {'to': toVol['uuid'], 'size': toVol['totalSize']}
            return

        if fromVol not in self.volumes:
            return

        fromVol = self.volumes[fromVol]
        fromParent = fromVol['parent']

        vols = [toVol for toVol in self.volumes.values()
                if toVol['parent'] == fromParent
                ]

        changeRate = self._calcChangeRate(vols)

        for toVol in vols:
            if toVol == fromVol:
                continue

            # This gives a conservative estimate of the size of the diff

            estimatedSize = self._estimateSize(toVol, fromVol, changeRate)

            yield {'to': toVol['uuid'], 'size': estimatedSize}

    def hasEdge(self, toUUID, fromUUID):
        """ Store already contains this edge. """
        return toUUID in self.volumes and fromUUID in self.volumes

    def _fullPath(self, path):
        return os.path.normpath(os.path.join(os.path.dirname(self.path), path))

    def receive(self, toUUID, fromUUID, volume, path):
        """ Return a file-like (stream) object to store a diff. """
        logger.debug("Receiving '%s'", path)
        return _Writer(self.butter.receive(), volume, self._fullPath(path))

    def _estimateSize(self, toVol, fromVol, changeRate):
        fromGen = fromVol['gen']
        genDiff = abs(toVol['gen'] - fromGen)

        estimatedSize = max(0, toVol['totalSize'] - fromVol['totalSize'])
        estimatedSize += toVol['totalSize'] * (1 - math.exp(-changeRate * genDiff))
        estimatedSize = max(toVol['exclusiveSize'], estimatedSize)

        return estimatedSize

    def _calcChangeRate(self, vols):
        total = 0
        diffs = 0
        minGen = vols[0]['gen']
        maxGen = minGen
        minSize = vols[0]['totalSize']
        maxSize = minSize

        for vol in vols:
            total += vol['totalSize']
            diffs += vol['exclusiveSize']
            minGen = min(minGen, vol['gen'])
            maxGen = max(maxGen, vol['gen'])
            minSize = min(minSize, vol['totalSize'])
            maxSize = max(maxSize, vol['totalSize'])

        try:
            # exclusiveSize is often useless,
            # because data may be shared with read-write volumes not usable for send operations
            diffs = max(diffs, maxSize - minSize)
            rate = - math.log(1 - diffs / total) * (len(vols) - 1) / (maxGen - minGen)
            rate /= 10  # Fudge
        except (ZeroDivisionError, ValueError):
            logger.debug("Using minimum change rate.")
            rate = theMinimumChangeRate

        logger.debug("Change rate: %f", rate)

        return rate

    def send(self, toUUID, fromUUID, streamContext, progress=True):
        """ Write the diff to the stream. """
        self.butter.send(toUUID, fromUUID, streamContext, progress)
