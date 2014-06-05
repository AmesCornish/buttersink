""" Manage read-only volumes in local btrfs file system. """

from __future__ import division

import butter

import math
import logging
logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')

theMinimumChangeRate = .0001


class FileSink:

    """ A local btrfs synchronization source or sink. """

    def __init__(self, host, path):
        """ Initialize.

        host is ignored.
        path is the file system location of the read-only subvolumes.
        """
        self.path = path
        self.butter = butter.Butter(path)
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

    def iterEdges(self, fromVol):
        """ Return the edges available from fromVol.

        Returned edge is a dict: 'to' UUID, estimated 'size' in MB
        """
        if fromVol is None:
            for toVol in self.volumes.values():
                yield {'to': toVol['uuid'], 'size': toVol['totalSize']}
            return

        fromVol = self.volumes[fromVol]
        fromParent = fromVol['parent']
        fromGen = fromVol['gen']

        vols = [toVol for toVol in self.volumes.values()
                if toVol['parent'] == fromParent
                ]

        # import pudb; pu.db

        rate = self._calcChangeRate(vols)

        logger.debug("Change rate: %f", rate)

        for toVol in vols:
            if toVol == fromVol:
                continue

            # This gives a conservative estimate of the size of the diff

            genDiff = abs(toVol['gen'] - fromGen)
            estimatedSize = toVol['exclusiveSize'] + \
                toVol['totalSize'] * (1 - math.exp(-rate * genDiff))

            yield {'to': toVol['uuid'], 'size': estimatedSize}

    def _calcChangeRate(self, vols):
        total = 0
        diffs = 0
        minGen = vols[0]['gen']
        maxGen = minGen

        for vol in vols:
            total += vol['totalSize']
            diffs += vol['exclusiveSize']
            minGen = min(minGen, vol['gen'])
            maxGen = max(maxGen, vol['gen'])

        try:
            rate = - math.log(1 - diffs / total) * (len(vols) - 1) / (maxGen - minGen)
        except ZeroDivisionError:
            logger.info("Using zero change rate.")
            rate = 0

        return max(rate, theMinimumChangeRate)

    def send(self, node):
        """ Use btrfs send to send a diff.

        Returns a stream object.
        """

        self.butter.send(node.uuid, node.previous)