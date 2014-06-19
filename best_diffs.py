""" Module to select best snapshot "send" commands to use for synchronization.

Based on optimzing a Directed Acyclic Graph (DAG),
where snapshots are the nodes,
and "send" diffs are the directed edges.
"""

# import pprint

import logging
logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')


class _Node:

    def __init__(self, uuid, intermediate=False):
        self.uuid = uuid
        self.intermediate = intermediate
        self.height = None
        self.totalCost = None
        self.previous = None
        self.diffSink = None
        self.diffCost = None
        self.diffSize = None

    def __unicode__(self):
        if self.diffSink is None:
            return u"<None>"

        volPath = self.diffSink.getVolume(self.uuid)['path']
        prevPath = self.diffSink.getVolume(self.previous)['path'] if self.previous else None

        return u"%s from %s (%f MB, %d ancestors)" % (
            volPath, prevPath, self.diffSize, self.height
            )

    def __str__(self):
        return unicode(self).encode('utf-8')

    @staticmethod
    def summary(nodes):
        count = 0
        cost = 0
        size = 0
        sinks = {}
        for n in nodes:
            count += 1
            cost += n.diffCost
            size += n.diffSize
            if n.diffSink in sinks:
                sinks[n.diffSink] += n.diffSize
            else:
                sinks[n.diffSink] = n.diffSize

        return {"count": count, "cost": cost, "size": size, "sinks": sinks}


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
        height = 0

        while len(nodes) > 0:
            logger.debug("Analyzing %d nodes at height %d...", len(nodes), height)
            for fromNode in nodes:
                if fromNode is not None and fromNode.height >= height:
                    continue

                fromCost = fromNode.totalCost if fromNode else 0
                fromUUID = fromNode.uuid if fromNode else None
                logger.debug("Examining edges from %s", fromUUID)

                for sink in sinks:
                    for edge in sink.iterEdges(fromUUID):
                        toUUID = edge['to']

                        # Skip any edges already in the destination
                        if sink != self.dest and self.dest.hasEdge(toUUID, fromUUID):
                            continue

                        # Get or create edge for this node
                        if toUUID in self.nodes:
                            toNode = self.nodes[toUUID]
                        else:
                            toNode = _Node(toUUID, True)
                            self.nodes[toUUID] = toNode

                        cost = self._cost(sink, edge['size'], height)

                        # Don't use a more-expensive path
                        if toNode.diffCost is not None and toNode.diffCost <= cost:
                            continue

                        # Don't create circular paths
                        if self._wouldLoop(fromUUID, toUUID):
                            logger.debug("Ignoring looping edge: %s", edge)
                            continue

                        logger.debug("Replacing edge %s...", toNode)

                        toNode.height = height
                        toNode.totalCost = fromCost + cost
                        toNode.previous = fromUUID
                        toNode.diffSink = sink
                        toNode.diffCost = cost
                        toNode.diffSize = edge['size']

                        logger.debug("...with better edge %s", toNode)

            nodes = [node for node in self.nodes.values() if node.height == height]
            height += 1

        self._prune()

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
        for node in self.nodes.values():
            yield node
            # yield { 'from': node.previous, 'to': node.uuid, 'sink': node.diffSink,
            # 'cost': node.diffCost }

    def summary(self):
        """ Return summary count and cost and size in a dictionary. """
        return _Node.summary(self.nodes.values())

    def _prune(self):
        """ Get rid of all intermediate nodes that aren't needed. """
        for node in [node for node in self.nodes.values() if node.intermediate]:
            if not [dep for dep in self.nodes if dep.previous == node]:
                self.nodes.remove(node)

    def _cost(self, sink, size, height):
        cost = 0

        # Transfer
        cost += size if sink != self.dest else 0

        # Storage
        cost += size if self.delete or sink != self.dest else 0

        # Corruption risk
        cost *= 2 ** (height/4.0)

        return cost
