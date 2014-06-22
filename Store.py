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

    def receive(self, toUUID, fromUUID, path):
        """ Return a file-like (stream) object to store a diff. """
        raise NotImplementedError

    def send(self, toUUID, fromUUID, stream):
        """ Write the diff (toUUID from fromUUID) to the stream. """
        raise NotImplementedError
