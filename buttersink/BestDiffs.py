""" Module to select best snapshot "send" commands to use for synchronization.

Based on optimzing a Directed Acyclic Graph (DAG),
where snapshots are the nodes,
and "send" diffs are the directed edges.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.
"""

# import pprint
import Store

import logging
logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')


class _Node:

    def __init__(self, uuid, intermediate=False):
        self.uuid = uuid
        self.intermediate = intermediate
        self.previous = None
        self.diffSink = None
        self.diffSize = None

    def __unicode__(self):
        try:
            ancestors = ", %d ancestors" % (self.height-1)
        except AttributeError:
            ancestors = ""

        return u"%s from %s (%s%s) in %s" % (
            self._getPath(self.uuid),
            self._getPath(self.previous),
            Store.humanize(self.diffSize),
            ancestors,
            self.diffSink
        )

    def __str__(self):
        return unicode(self).encode('utf-8')

    @staticmethod
    def getPath(node):
        """ Return printable description of node, if not None. """
        if node is None:
            return None
        uuid = node.uuid
        return node._getPath(uuid)

    def _getPath(self, uuid):
        """ Return printable description of uuid. """
        result = Store.printUUID(uuid)
        try:
            result = "%s (%s)" % (self.diffSink.getVolume(uuid)['path'], result)
        except (KeyError, AttributeError):
            pass
        return result

    @staticmethod
    def summary(nodes):
        count = 0
        size = 0
        sinks = {}
        for n in nodes:
            count += 1

            if n.diffSize is not None:
                size += n.diffSize
                if n.diffSink in sinks:
                    sinks[n.diffSink] += n.diffSize
                else:
                    sinks[n.diffSink] = n.diffSize

        return {"count": count, "size": size, "sinks": sinks}


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

    def analyze(self, *sinks):
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
                fromUUID = fromNode.uuid if fromNode else None

                logger.debug(
                    "Following edges from %s (total %s)",
                    _Node.getPath(fromNode), Store.humanize(fromSize)
                )

                for sink in sinks:
                    logger.debug(
                        "Listing edges in %s",
                        sink
                    )

                    for edge in sink.getEdges(fromUUID):
                        toUUID = edge['to']

                        # Skip any edges already in the destination
                        if sink != self.dest and self.dest.hasEdge(toUUID, fromUUID):
                            continue

                        if toUUID in self.nodes:
                            toNode = self.nodes[toUUID]
                        else:
                            toNode = _Node(toUUID, True)
                            self.nodes[toUUID] = toNode

                        newCost = self._cost(sink, edge['size'], fromSize, height)
                        if toNode.diffSink is None:
                            oldCost = None
                        else:
                            oldCost = self._cost(
                                toNode.diffSink,
                                toNode.diffSize,
                                fromSize,
                                self._height(toNode)
                                )

                        # Don't use a more-expensive path
                        if oldCost is not None and oldCost <= newCost:
                            continue

                        # Don't create circular paths
                        if self._wouldLoop(fromUUID, toUUID):
                            logger.debug("Ignoring looping edge: %s", toNode._getPath(edge['to']))
                            continue

                        logger.debug(
                            "Replacing edge (%s -> %s cost) %s",
                            Store.humanize(oldCost),
                            Store.humanize(newCost),
                            toNode
                        )

                        toNode.previous = fromUUID
                        toNode.diffSink = sink
                        toNode.diffSize = edge['size']

            nodes = [node for node in self.nodes.values() if self._height(node) == height]
            height += 1

        self._prune()

        for node in self.nodes.values():
            node.height = self._height(node)

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

    def _getNode(self, uuid):
        return self.nodes[uuid] if uuid is not None else None

    def _wouldLoop(self, fromNode, toNode):
        if toNode is None:
            return False

        while fromNode is not None:
            if fromNode == toNode:
                return True

            fromNode = self.nodes[fromNode].previous

        return False

    def iterDiffs(self):
        """ Return all diffs used in optimal network. """
        nodes = self.nodes.values()
        nodes.sort(key=lambda node: self._height(node))
        for node in nodes:
            yield node
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
                if not [dep for dep in self.nodes.values() if dep.previous == node.uuid]:
                    logger.debug("Removing unnecessary node %s", _Node.getPath(node))
                    del self.nodes[node.uuid]
                    done = False

    def _cost(self, sink, size, prevSize, height):
        cost = 0

        # Transfer
        cost += size if sink != self.dest else 0

        # Storage
        cost += size if self.delete or sink != self.dest else 0

        # Corruption risk
        cost += (prevSize + size) * (2 ** (height - 8))

        return cost
