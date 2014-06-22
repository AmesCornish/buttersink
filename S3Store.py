""" Back-end for AWS S3. """

# Docs: http://boto.readthedocs.org/en/latest/

from __future__ import division

if True:  # Imports and constants
    if True:  # Imports
        import Store

        import boto
        import io
        import logging
        import pprint
        import re
        import sys
    if True:  # Constants
        # For S3 uploads
        theChunkSize = 100 * 2**20

        # Maximum xumber of progress reports per chunk
        theProgressCount = 100

        # This does transparent S3 server-side encryption
        isEncrypted = True

        # TODO: Get a real version number
        theBtrfsVersion = '0.0'

        logger = logging.getLogger(__name__)
        # logger.setLevel('DEBUG')


class S3Store(Store.Store):

    """ An S3 bucket synchronization source or sink. """

    def __init__(self, host, path):
        """ Initialize.

        host is the bucket name.
        path is an object key prefix to use.
        """
        self.bucketName = host

        path = path.strip("/")
        if path:
            path += "/"
        self.prefix = path

        self.keyPattern = re.compile(S3Store.theKeyPattern % ())

        # List of dict with from, to, size, path
        self.diffs = None

        # { uuid: { path, uuid, } }
        self.vols = None

        logger.info("Listing %s contents...", self)

        s3 = boto.connect_s3()
        self.bucket = s3.get_bucket(self.bucketName)
        self._listBucket()

    def __unicode__(self):
        """ Return text description. """
        return u'S3 Bucket "%s"' % (self.bucketName)

    def __str__(self):
        """ Return text description. """
        return unicode(self).encode('utf-8')

    def _listBucket(self):
        self.vols = {}
        self.diffs = []

        for key in self.bucket.list():
            diff = self._parseKeyName(key.name)

            if diff is None:
                logger.warn("Can't parse '%s' in S3", key.name)
                continue

            if diff['from'] == 'None':
                diff['from'] = None

            extra = not diff['fullpath'].startswith(self.prefix.rstrip("/"))

            if not extra:
                diff['path'] = diff['fullpath'][len(self.prefix):]
            else:
                diff['path'] = diff['fullpath']

            if not diff['path']:
                diff['path'] = "."

            self.vols[diff['to']] = {
                'uuid': diff['to'],
                'path': diff['path'],
                'fullpath': diff['fullpath'],
                'extra': extra,
                }

            self.diffs.append({
                'from': diff['from'],
                'to': diff['to'],
                'size': key.size / (2**20),
                'path': diff['path']
                })

        logger.debug("Diffs:\n%s", pprint.pformat(self.diffs))
        logger.debug("Vols:\n%s", pprint.pformat(self.vols))

    def listVolumes(self):
        """ Return list of volumes that are available. """
        return [vol for vol in self.vols.values() if not vol['extra']]

    def getVolume(self, uuid):
        """ Return info about volume. """
        return self.vols[uuid]

    def iterEdges(self, fromVol):
        """ Return the edges available from fromVol.

        Returned edge is a dict: 'to' UUID, estimated 'size' in MB
        """
        for diff in self.diffs:
            if diff['from'] == fromVol:
                yield {'to': diff['to'], 'size': diff['size']}

    def hasEdge(self, toUUID, fromUUID):
        """ Test whether edge is in this sink. """
        for diff in self.diffs:
            if diff['from'] == fromUUID and diff['to'] == toUUID:
                return True
        return False

    def receive(self, toUUID, fromUUID, path):
        """ Return a file-like (stream) object to store a diff. """
        return _Uploader(self.bucket, self._keyName(toUUID, fromUUID, self.prefix + path))

    theKeyPattern = "^(?P<fullpath>.*)/(?P<to>[-a-zA-Z0-9]*)_(?P<from>[-a-zA-Z0-9]*)$"

    def _keyName(self, toUUID, fromUUID, path):
        return "%s/%s_%s" % (path, toUUID, fromUUID)

    def _parseKeyName(self, name):
        match = self.keyPattern.match(name)
        return match.groupdict() if match else None

    def send(self, toUUID, fromUUID, stream):
        """ Write the diff to the stream. """
        path = self.vols[toUUID]['fullpath']
        key = self.bucket.get_key(self._keyName(toUUID, fromUUID, path))
        key.get_contents_to_file(stream, cb=_displayProgress, num_cb=theProgressCount)

    def _upload(self, stream, keyName):
        # key = self.bucket.get_key(keyName)
        # key = self.bucket.new_key(keyName)

        # set_contents_from_stream is not supported for S3
        # key.set_contents_from_stream(stream, replace=False, cb=displayProgress, size=1000)
        # key.set_contents_from_filename(fileName, replace=False, cb=displayProgress)

        with _Uploader(self.bucket, keyName) as uploader:
            while True:
                data = stream.read(theChunkSize)
                if not data:
                    break
                uploader.upload(data)


def _displayProgress(sent, total):
    if not sys.stdout.isatty():
        return
    sys.stdout.write(
        "\rSent %.3g of %.3g MiB (%d%%) %20s" %
        (sent / (2**20), total / (2**20), int(100*sent/total), " ")
        )
    if sent == total:
        sys.stdout.write("\n")
    sys.stdout.flush()


class _Uploader:
    def __init__(self, bucket, keyName):
        self.bucket = bucket
        self.keyName = keyName
        self.uploader = None
        self.chunkCount = None
        self.open()

    def __enter__(self):
        return self

    def open(self):
        self.uploader = self.bucket.initiate_multipart_upload(
            self.keyName,
            encrypt_key=isEncrypted,
            metadata={'btrfsVersion': theBtrfsVersion},
            )
        self.chunkCount = 0

    def __exit__(self, exceptionType, exceptionValue, traceback):
        self.close(abort=exceptionType is not None)
        return False  # Don't supress exception

    def write(self, bytes):
        self.upload(bytes)

    def close(self, abort=False):
        if not abort:
            self.uploader.complete_upload()
        else:
            # TODO: this doesn't free storage used by part uploads currently in progress
            self.uploader.cancel_upload()
        self.uploader = None

    def fileno(self):
        raise IOError("S3 uploads don't use file numbers.")

    def upload(self, bytes):
        self.chunkCount += 1
        logger.info(
            "Uploading %.3g MiB chunk #%d for %s",
            len(bytes)/(2**20), self.chunkCount, self.keyName
            )
        fileObject = io.BytesIO(bytes)
        self.uploader.upload_part_from_file(
            fileObject, self.chunkCount, cb=_displayProgress, num_cb=theProgressCount
            )
