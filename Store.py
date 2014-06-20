""" Abstract and component classes for sources and sinks. """

# sink, source, src, dest: store
# volume, diff

# Classes: CamelCase
# files: CamelCase.py
# project: butter_sink

# Volume: { uuid, path, size }
# Diff: { toUUID, fromUUID, size }

class Volume:

    """ Stored snapshot. """

    def __init__(self, uuid, path, size=None):
        """ Initialize.

        uuid: globally unique id, suitable for indexing
        path: local relative path, may change
        size: in MiB, if known
        """
        self.uuid = uuid
        self.uuid = uuid
        self.uuid = uuid


class Store:

    """ Abstract class for storage back-end. """

    pass
