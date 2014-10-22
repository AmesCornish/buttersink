""" Module to select best snapshot "send" commands to use for synchronization.

Based on optimzing a Directed Acyclic Graph (DAG),
where snapshots are the nodes,
and "send" diffs are the directed edges.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

import Store

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

        return sinks


class BestDiffs:

    """ This analyzes and stores an optimal network (tree).

    The nodes are the desired (or intermediate) volumes.
    The directed edges are diffs from an available sink.

    """

    def __init__(self, volumes, delete=False):
        """ Initialize.

        volumes are the required snapshots.

        """
        self.nodes = {volume: _Node(volume, False) for volume in volumes}
        self.dest = None
        self.delete = delete

    def analyze(self, chunkSize, *sinks):
        """  Figure out the best diffs to use to reach all our required volumes. """
        # Use destination (already uploaded) edges first
        sinks = list(sinks)
        sinks.reverse()
        self.dest = sinks[0]

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

                fromSize = self._totalSize(fromNode)
                fromVol = fromNode.volume if fromNode else None

                logger.debug(
                    "Following edges from %s (total %s)",
                    fromNode.display(sinks[-1]) if fromNode is not None else None,
                    Store.humanize(fromSize),
                )

                for sink in sinks:
                    logger.debug(
                        "Listing edges in %s",
                        sink
                    )

                    for edge in sink.getEdges(fromVol):
                        toVol = edge.toVol

                        logger.debug("Edge: %s", edge)

                        # Skip any edges already in the destination
                        if sink != self.dest and self.dest.hasEdge(edge):
                            continue

                        if toVol in self.nodes:
                            toNode = self.nodes[toVol]
                        else:
                            toNode = _Node(toVol, True)
                            self.nodes[toVol] = toNode

                        newCost = self._cost(sink, edge.size, fromSize, height)
                        if toNode.diff is None:
                            oldCost = None
                        else:
                            oldCost = self._cost(
                                toNode.sink,
                                toNode.diffSize,
                                fromSize,
                                self._height(toNode)
                            )

                        # Don't use a more-expensive path
                        if oldCost is not None and oldCost <= newCost:
                            continue

                        # Don't create circular paths
                        if self._wouldLoop(fromVol, toVol):
                            logger.debug("Ignoring looping edge: %s", toVol.display(sink))
                            continue

                        if sink != self.dest and edge.sizeIsEstimated:
                            sink.measureSize(edge, chunkSize)
                            newCost = self._cost(sink, edge.size, fromSize, height)
                            if oldCost is not None and oldCost <= newCost:
                                continue

                        logger.debug(
                            "Replacing edge (%s -> %s cost) %s",
                            Store.humanize(oldCost),
                            Store.humanize(newCost),
                            toNode.display(sink)
                        )

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
                    logger.debug("Removing unnecessary node %s", node)
                    del self.nodes[node.volume]
                    done = False

    def _cost(self, sink, size, prevSize, height):
        cost = 0

        # Transfer
        if sink != self.dest:
            cost += size

        # Storage
        if sink != self.dest or self.delete:
            cost += size / 16

        # Corruption risk
        cost += (prevSize + size) * (2 ** (height - 8))

        logger.debug("_cost=%d (%s %d %d %d)", cost, sink, size, prevSize, height)

        return cost
