""" Manage read-only volumes in local btrfs file system. """

from __future__ import division

import Butter
import Store

import math
import logging
logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')

theMinimumChangeRate = .0001


class ButterStore(Store.Store):

    """ A local btrfs synchronization source or sink. """

    def __init__(self, host, path):
        """ Initialize.

        host is ignored.
        path is the file system location of the read-only subvolumes.
        """
        self.path = path
        self.butter = Butter.Butter(path)
        self.volumes = self.butter.listVolumes()

    def __unicode__(self):
        """ English description of self. """
        return u"btrfs snapshots in %s" % (self.path)

    def __str__(self):
        """ English description of self. """
        return unicode(self).encode('utf-8')

    def listVolumes(self):
        """ Return list of volumes that are available. """
        return self.volumes.values()

    def getVolume(self, uuid):
        """ Return dict of info for a specific volume. """
        return self.volumes[uuid]

    def iterEdges(self, fromVol):
        """ Return the edges available from fromVol.

        Returned edge is a dict: 'to' UUID, estimated 'size' in MB
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

    def receive(self, toUUID, fromUUID, stream):
        """ Store the diff. """
        self.butter.receive(stream)

    def _estimateSize(self, toVol, fromVol, changeRate):
        fromGen = fromVol['gen']
        genDiff = abs(toVol['gen'] - fromGen)

        estimatedSize = max(toVol['exclusiveSize'], toVol['totalSize'] - fromVol['totalSize'])
        estimatedSize += toVol['totalSize'] * (1 - math.exp(-changeRate * genDiff))

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
        except ZeroDivisionError:
            logger.info("Using zero change rate.")
            rate = 0

        rate = max(rate, theMinimumChangeRate)

        logger.debug("Change rate: %f", rate)

        return rate

    def send(self, node):
        """ Use btrfs send to send a diff.

        Returns a stream object.
        """
        return self.butter.send(node.uuid, node.previous)

