""" Back-end for AWS S3.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

# Docs: http://boto.readthedocs.org/en/latest/

from __future__ import division

if True:  # Imports and constants
    if True:  # Imports
        import Store

        import boto
        import collections
        import datetime
        import io
        import logging
        import os.path
        import pprint
        import re
        import sys
    if True:  # Constants
        # For S3 uploads
        theChunkSize = 100 * 2 ** 20

        # Maximum xumber of progress reports per chunk
        theProgressCount = 50

        # This does transparent S3 server-side encryption
        isEncrypted = True

        logger = logging.getLogger(__name__)
        # logger.setLevel('DEBUG')


class S3Store(Store.Store):

    """ An S3 bucket synchronization source or sink. """

    def __init__(self, host, path, isDest):
        """ Initialize.

        host is the bucket name.
        path is an object key prefix to use.

        """
        super(S3Store, self).__init__()

        self.isDest = isDest

        self.bucketName = host

        if path:
            path = path.strip("/")
        if path:
            path += "/"
        else:
            path = ""
        self.prefix = path

        self.keyPattern = re.compile(S3Store.theKeyPattern % ())

        # { fromVol: [diff] }
        self.diffs = None

        logger.info("Listing %s contents...", self)

        try:
            s3 = boto.connect_s3()
        except boto.exception.NoAuthHandlerFound:
            logger.error("Try putting S3 credentials into ~/.boto")
            raise

        self.bucket = s3.get_bucket(self.bucketName)
        self._fillVolumesAndPaths()

    def __unicode__(self):
        """ Return text description. """
        return u'S3 Bucket "%s"' % (self.bucketName)

    def __str__(self):
        """ Return text description. """
        return unicode(self).encode('utf-8')

    def _fillVolumesAndPaths(self):
        """ Fill in self.paths. """
        self.diffs = collections.defaultdict((lambda: []))

        for key in self.bucket.list():
            keyInfo = self._parseKeyName(key.name)

            if keyInfo is None:
                if key.name[-1:] != '/':
                    logger.warn("Ignoring '%s' in S3", key.name)
                continue

            if keyInfo['from'] == 'None':
                keyInfo['from'] = None

            if keyInfo['fullpath'].startswith(self.prefix.rstrip("/")):
                path = keyInfo['fullpath'][len(self.prefix):]
                assert not path or path[0] != '/', path  # Indicates relative path
            else:
                path = "/" + keyInfo['fullpath']
                assert path[0] == '/', path  # Indicates absolute path

            if not path:
                path = "."

            logger.debug("Adding %s", path)

            diff = Store.Diff(self, keyInfo['to'], keyInfo['from'], key.size)

            self.diffs[diff.fromVol].append(diff)
            self.paths[diff.toVol].add(path)

        # logger.debug("Diffs:\n%s", pprint.pformat(self.diffs))
        # logger.debug("Vols:\n%s", pprint.pformat(self.vols))

    def getEdges(self, fromVol):
        """ Return the edges available from fromVol. """
        return self.diffs[fromVol]

    def hasEdge(self, diff):
        """ Test whether edge is in this sink. """
        return diff.toVol in [d.toVol for d in self.diffs[diff.fromVol]]

    def receive(self, diff, paths):
        """ Return Context Manager for a file-like (stream) object to store a diff. """
        path = self.selectPah(paths)
        path = os.path.normpath(os.path.join(self.prefix, path))
        key = self._keyName(diff.toVol.uuid, diff.fromVol.uuid, path)
        return _Uploader(self.bucket, key)

    theKeyPattern = "^(?P<fullpath>.*)/(?P<to>[-a-zA-Z0-9]*)_(?P<from>[-a-zA-Z0-9]*)$"

    def _keyName(self, toUUID, fromUUID, path):
        return "%s/%s_%s" % (path, toUUID, fromUUID)

    def _parseKeyName(self, name):
        """ Returns dict with fullpath, to, from. """
        match = self.keyPattern.match(name)
        return match.groupdict() if match else None

    def send(self, toUUID, fromUUID, streamContext, progress=True):
        """ Write the diff to the streamContext. """
        path = self.vols[toUUID]['fullpath']
        key = self.bucket.get_key(self._keyName(toUUID, fromUUID, path))

        with streamContext as stream:
            if progress:
                key.get_contents_to_file(stream, cb=_DisplayProgress(), num_cb=theProgressCount)
            else:
                key.get_contents_to_file(stream)

        if sys.stdout.isatty():
            sys.stdout.write("\n")

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


class _DisplayProgress:
    def __init__(self):
        self.startTime = datetime.datetime.now()

    def __call__(self, sent, total):
        if not sys.stdout.isatty():
            return

        elapsed = datetime.datetime.now() - self.startTime
        mbps = (sent * 8 / (10**6) / elapsed.total_seconds())

        sys.stdout.write(
            "\r %s: Sent %s of %s (%d%%) (%.3g Mbps) %20s\r" % (
                elapsed,
                Store.humanize(sent),
                Store.humanize(total),
                int(100 * sent / total),
                mbps,
                " ",
                )
        )

        sys.stdout.flush()


class _Uploader:

    def __init__(self, bucket, keyName):
        self.bucket = bucket
        self.keyName = keyName
        self.uploader = None
        self.chunkCount = None
        self.metadata = {}
        self.progress = True

    def __enter__(self):
        self.open()
        return self

    def open(self):
        self.uploader = self.bucket.initiate_multipart_upload(
            self.keyName,
            encrypt_key=isEncrypted,
            metadata=self.metadata,
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
            # You cannot change metadata after uploading
            # if self.metadata:
            #     key = self.bucket.get_key(self.keyName)
            #     key.update_metadata(self.metadata)
        else:
            # TODO: this doesn't free storage used by part uploads currently in progress
            self.uploader.cancel_upload()
        self.uploader = None

    def fileno(self):
        raise IOError("S3 uploads don't use file numbers.")

    def upload(self, bytes):
        self.chunkCount += 1
        logger.info(
            "Uploading %s chunk #%d for %s",
            Store.humanize(len(bytes)), self.chunkCount, self.keyName
        )
        fileObject = io.BytesIO(bytes)

        if self.progress:
            self.uploader.upload_part_from_file(
                fileObject, self.chunkCount, cb=_DisplayProgress(), num_cb=theProgressCount
            )
            if sys.stdout.isatty():
                sys.stdout.write("\n")
        else:
            self.uploader.upload_part_from_file(
                fileObject, self.chunkCount
            )
