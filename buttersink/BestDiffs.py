""" Module to select best snapshot "send" commands to use for synchronization.

Based on optimzing a Directed Acyclic Graph (DAG),
where snapshots are the nodes,
and "send" diffs are the directed edges.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

from util import humanize

import collections
import logging
logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')


class Bunch(object):

    """ Simple mutable data record. """

    def __init__(self, **kwds):
        """ Initialize. """
        self.__dict__.update(kwds)


class _Node:

    def __init__(self, volume, intermediate=False):
        self.volume = volume
        self.intermediate = intermediate
        self.diff = None
        self.height = None

    @property
    def diffSize(self):
        return self.diff.size if self.diff is not None else None

    @property
    def previous(self):
        return self.diff.fromVol if self.diff else None

    @property
    def sink(self):
        return self.diff.sink if self.diff else None

    def __unicode__(self):
        return self.display()

    def display(self, sink=None):
        if self.height is not None:
            ancestors = " (%d ancestors)" % (self.height - 1)
        else:
            ancestors = ""

        return u"%s%s" % (
            self.diff or self.volume.display(sink),
            ancestors,
        )

    def __str__(self):
        return unicode(self).encode('utf-8')

    @staticmethod
    def summary(nodes):
        sinks = collections.defaultdict(lambda: Bunch(count=0, size=0))

        total = sinks[None]

        for n in nodes:
            total.count += 1

            if n.diff is None:
                continue

            total.size += n.diff.size

            sink = sinks[n.diff.sink]
            sink.count += 1
            sink.size += n.diff.size

        sinksSorted = collections.OrderedDict(
            {s: b for s, b in sinks.items() if s is not None}
        )
        sinksSorted[None] = sinks[None]
        return sinksSorted


class BestDiffs:

    """ This analyzes and stores an optimal network (tree).

    The nodes are the desired (or intermediate) volumes.
    The directed edges are diffs from an available sink.

    """

    def __init__(self, volumes, delete=False, measureSize=True):
        """ Initialize.

        volumes are the required snapshots.

        """
        self.nodes = {volume: _Node(volume, False) for volume in volumes}
        self.dest = None
        self.delete = delete
        self.measureSize = measureSize

    def analyze(self, chunkSize, *sinks):
        """  Figure out the best diffs to use to reach all our required volumes. """
        measureSize = False
        if self.measureSize:
            for sink in sinks:
                if sink.isRemote:
                    measureSize = True

        # Use destination (already uploaded) edges first
        sinks = list(sinks)
        sinks.reverse()
        self.dest = sinks[0]

        def currentSize():
            return sum([
                n.diffSize
                for n in self.nodes.values()
                if n.diff is not None and n.diff.sink != self.dest
            ])

        while True:
            self._analyzeDontMeasure(chunkSize, measureSize, *sinks)

            if not measureSize:
                return

            estimatedSize = currentSize()

            # logger.info("Measuring any estimated diffs")

            for node in self.nodes.values():
                edge = node.diff
                if edge is not None and edge.sink != self.dest and edge.sizeIsEstimated:
                    edge.sink.measureSize(edge, chunkSize)

            actualSize = currentSize()

            logger.info(
                "measured size (%s), estimated size (%s)",
                humanize(actualSize), humanize(estimatedSize),
            )

            if actualSize <= 1.2 * estimatedSize:
                return

    def _analyzeDontMeasure(self, chunkSize, willMeasureLater, *sinks):
        """  Figure out the best diffs to use to reach all our required volumes. """
        nodes = [None]
        height = 1

        def sortKey(node):
            if node is None:
                return None
            return (node.intermediate, self._totalSize(node))

        while len(nodes) > 0:
            logger.debug("Analyzing %d nodes for height %d...", len(nodes), height)

            nodes.sort(key=sortKey)

            for fromNode in nodes:
                if self._height(fromNode) >= height:
                    continue

                if fromNode is not None and fromNode.diffSize is None:
                    continue

                fromVol = fromNode.volume if fromNode else None

                logger.debug("Following edges from %s", fromVol)

                for sink in sinks:
                    # logger.debug(
                    #     "Listing edges in %s",
                    #     sink
                    # )

                    for edge in sink.getEdges(fromVol):
                        toVol = edge.toVol

                        # logger.debug("Edge: %s", edge)

                        # Skip any edges already in the destination
                        if sink != self.dest and self.dest.hasEdge(edge):
                            continue

                        if toVol in self.nodes:
                            toNode = self.nodes[toVol]
                        # Don't transfer any edges we won't need in the destination
                        # elif sink != self.dest:
                        #     logger.debug("Won't transfer unnecessary %s", edge)
                        #     continue
                        else:
                            toNode = _Node(toVol, True)
                            self.nodes[toVol] = toNode

                        logger.debug("Considering %s", edge)

                        edgeSize = edge.size
                        if edge.sizeIsEstimated:
                            if willMeasureLater:
                                # Slight preference for accurate sizes
                                edgeSize *= 1.2
                            else:
                                # Large preference for accurate sizes
                                edgeSize *= 2

                        newCost = self._cost(sink, edgeSize, fromNode, height)

                        if toNode.diff is None:
                            oldCost = None
                        else:
                            oldCost = self._cost(
                                toNode.sink,
                                toNode.diffSize,
                                self._getNode(toNode.previous),
                                self._height(toNode)
                            )

                        # Don't use a more-expensive path
                        if oldCost is not None and oldCost <= newCost:
                            continue

                        # Don't create circular paths
                        if self._wouldLoop(fromVol, toVol):
                            # logger.debug("Ignoring looping edge: %s", toVol.display(sink))
                            continue

                        # if measureSize and sink != self.dest and edge.sizeIsEstimated:
                        #     sink.measureSize(edge, chunkSize)
                        #     newCost = self._cost(sink, edge.size, fromSize, height)
                        #     if oldCost is not None and oldCost <= newCost:
                        #         continue

                        logger.debug(
                            "Replacing edge (%s -> %s cost)\n%s",
                            humanize(oldCost),
                            humanize(newCost),
                            toNode.display(sink)
                        )
                        # logger.debug("Cost elements: %s", dict(
                        #     sink=str(sink),
                        #     edgeSize=humanize(edgeSize),
                        #     fromSize=humanize(fromSize),
                        #     height=height,
                        # ))

                        toNode.diff = edge

            nodes = [node for node in self.nodes.values() if self._height(node) == height]
            height += 1

        self._prune()

        for node in self.nodes.values():
            node.height = self._height(node)
            if node.diff is None:
                logger.error(
                    "No source diffs for %s",
                    node.volume.display(sinks[-1], detail="line"),
                )

    def _getNode(self, vol):
        return self.nodes[vol] if vol is not None else None

    def _height(self, node):
        if node is None:
            return 0
        else:
            return 1 + self._height(self._getNode(node.previous))

    def _totalSize(self, node):
        if node is None:
            return 0

        prevSize = self._totalSize(self._getNode(node.previous))

        return (node.diffSize or 0) + prevSize

    def _wouldLoop(self, fromVol, toVol):
        if toVol is None:
            return False

        while fromVol is not None:
            if fromVol == toVol:
                return True

            fromVol = self.nodes[fromVol].previous

        return False

    def iterDiffs(self):
        """ Return all diffs used in optimal network. """
        nodes = self.nodes.values()
        nodes.sort(key=lambda node: self._height(node))
        for node in nodes:
            yield node.diff
            # yield { 'from': node.previous, 'to': node.uuid, 'sink': node.diffSink,

    def summary(self):
        """ Return summary count and size in a dictionary. """
        return _Node.summary(self.nodes.values())

    def _prune(self):
        """ Get rid of all intermediate nodes that aren't needed. """
        done = False
        while not done:
            done = True
            for node in [node for node in self.nodes.values() if node.intermediate]:
                if not [dep for dep in self.nodes.values() if dep.previous == node.volume]:
                    # logger.debug("Removing unnecessary node %s", node)
                    del self.nodes[node.volume]
                    done = False

    def _cost(self, sink, size, prevNode, height):
        cost = 0
        prevSize = self._totalSize(prevNode)

        # Transfer
        if sink != self.dest:
            cost += size
            if prevNode is not None and prevNode.intermediate and prevNode.sink != self.dest:
                cost += prevSize

        # Storage
        if sink != self.dest or self.delete:
            cost += size / 16

        # Corruption risk
        cost += (prevSize + size) * (2 ** (height - 6))

        logger.debug(
            "_cost=%s (%s %s %s %d)",
            humanize(cost), sink, humanize(size), humanize(prevSize), height,
        )

        return cost
