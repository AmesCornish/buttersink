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

    def __unicode__(self):
        return u"%s from %s (%f MB)" % (self.uuid, self.previous, self.diffCost)

    def __str__(self):
        return unicode(self).encode('utf-8')


class BestDiffs:

    """ This analyzes and stores an optimal network (tree).

    The nodes are the desired (or intermediate) volumes.
    The directed edges are diffs from an available sink.
    """

    def __init__(self, volumes):
        self.nodes = { volume : _Node(volume, False) for volume in volumes }
        self.dest = None

    def analyze(self, *sinks):
        '''
        Figure out the best diffs to use to reach all our volumes.
        '''

        self.dest = sinks[-1]

        nodes = [ None ]
        height = 0

        while len(nodes) > 0:
            logger.info("Analyzing %d nodes at height %d", len(nodes), height)
            for fromNode in nodes:
                fromCost = fromNode.totalCost if fromNode else 0
                fromUUID = fromNode.uuid if fromNode else None
                for sink in sinks:
                    for edge in sink.iterEdges(fromUUID):
                        toUUID = edge['to']
                        if toUUID in self.nodes:
                            toNode = self.nodes[toUUID]
                        else:
                            toNode = _Node(toUUID, True)
                            self.nodes[toUUID] = toNode

                        cost = self._cost(sink, edge['size'], height)

                        # Don't replace an identical diff already uploaded to destination
                        if toNode.previous == fromUUID and toNode.diffSink == self.dest:
                            continue

                        # Don't use a more-expensive path
                        if toNode.diffCost is not None and toNode.diffCost <= cost:
                            # Unless it's an identical diff already uploaded
                            if toNode.previous != fromUUID or sink != self.dest:
                                continue

                        # Don't create circular paths
                        if self._wouldLoop(fromUUID, toUUID):
                            logger.debug("Ignoring looping edge: %s", edge)
                            continue

                        toNode.height = height
                        toNode.totalCost = fromCost + cost
                        toNode.previous = fromUUID
                        toNode.diffSink = sink
                        toNode.diffCost = cost

                        logger.debug("Found useful edge %s", toNode)

            nodes = [ node for node in self.nodes.values() if node.height == height ]
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
        '''
        Return all diffs used in optimal network.
        '''
        for node in self.nodes.values():
            yield node
            # yield { 'from': node.previous, 'to': node.uuid, 'sink': node.diffSink, 'cost': node.diffCost }

    def _prune(self):
        '''
        Get rid of all intermediate nodes that aren't needed.
        '''

        for node in [node for node in self.nodes.values() if node.intermediate]:
            if not [ dep for dep in self.nodes if dep.previous == node ]:
                self.nodes.remove(node)

    def _cost(self, sink, size, height):
        cost = 0
        delete = True

        # Transfer
        cost += size if sink != self.dest else 0

        # Storage
        cost += size if delete or sink != self.dest else 0

        # Corruption risk
        cost *= 2**height

        return cost
