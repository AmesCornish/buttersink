""" Abstract and component classes for sources and sinks. """

# sink, source, src, dest: store
# volume, diff

# Classes: CamelCase
# files: CamelCase.py
# project: butter_sink

# Volume: { uuid, path, size }
# Diff: { toUUID, fromUUID, size }


class Store:

    """ Abstract class for storage back-end. """

    def hasEdge(self, toUUID, fromUUID):
        """ Store already contains this edge. """
        raise NotImplementedError

    def receive(self, toUUID, fromUUID, stream):
        """ Store the diff. """
        raise NotImplementedError

    def send(self, node):
        """ Return a stream object to get the diff. """
        raise NotImplementedError
