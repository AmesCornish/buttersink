import logging
logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

class _Node:
    def __init__(self, uuid, intermediate=False):
        self.uuid = uuid
        self.intermediate = intermediate
        self.height = None
        self.totalCost = None
        self.previous = None
        self.diffSink = None
        self.diffCost = None

class BestDiffs:
    '''
    This analyzes and stores an optimal network (tree).
    The nodes are the desired (or intermediate) volumes.
    The directed edges are diffs from an available sink.
    '''

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
                for sink in sinks:
                    for edge in sink.iterEdges(fromNode.uuid if fromNode else None):
                        toUUID = edge['to']
                        if toUUID in self.nodes:
                            toNode = self.nodes[toUUID]
                        else:
                            toNode = _Node(toUUID, True)
                            self.nodes[toUUID] = toNode

                        cost = self._cost(sink, edge['size'], height)

                        if toNode.totalCost <= fromCost + cost:
                            continue

                        toNode.height = height
                        toNode.totalCost = fromCost + cost
                        toNode.previous = fromNode
                        toNode.diffSink = sink
                        toNode.diffCost = cost

            nodes = [ node for node in self.nodes if node.height == height ]
            height += 1

        self._prune()

    def iterDiffs(self):
        '''
        Return all diffs used in optimal network.
        '''
        for node in self.nodes:
            yield { 'from': node.previous, 'to': node.uuid, 'sink': node.diffSink, 'cost': node.diffCost }

    def _prune(self):
        '''
        Get rid of all intermediate nodes that aren't needed.
        '''

        for node in [node for node in self.nodes if node.intermediate]:
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
