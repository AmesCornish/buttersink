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
        import re
        import sys
    if True:  # Constants
        # For S3 uploads
        theChunkSize = 100 * 2 ** 20

        # Maximum xumber of progress reports per chunk
        theProgressCount = 50

        # This does transparent S3 server-side encryption
        isEncrypted = True

        # Minimum chuck size is 5M.
        theInfoBufferSize = 10 * (1 << 20)

        logger = logging.getLogger(__name__)

# logger.setLevel('DEBUG')


class S3Store(Store.Store):

    """ An S3 bucket synchronization source or sink. """

    def __init__(self, host, path, isDest, dryrun):
        """ Initialize.

        host is the bucket name.
        path is an object key prefix to use.

        """
        super(S3Store, self).__init__("/" + path, isDest, dryrun)

        self.isDest = isDest

        self.bucketName = host

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
        self._flushPartialUploads()
        self._fillVolumesAndPaths()

    def __unicode__(self):
        """ Return text description. """
        return u'S3 Bucket "%s"' % (self.bucketName)

    def __str__(self):
        """ Return text description. """
        return unicode(self).encode('utf-8')

    def _flushPartialUploads(self):
        for upload in self.bucket.get_all_multipart_uploads():
            if self.dryrun:
                logger.warn("Old partial upload: %s", upload)
            else:
                logger.warn("Cancelling old partial upload: %s", upload)
                upload.cancel_upload()

    def _fillVolumesAndPaths(self):
        """ Fill in self.paths. """
        self.diffs = collections.defaultdict((lambda: []))

        for key in self.bucket.list():
            keyInfo = self._parseKeyName(key.name)

            if keyInfo is None:
                if key.name[-1:] != '/':
                    logger.warn("Ignoring '%s' in S3", key.name)
                continue

            if keyInfo['type'] == 'info':
                stream = io.BytesIO()
                key.get_contents_to_file(stream)
                Store.Volume.readInfo(stream)
                continue

            if keyInfo['from'] == 'None':
                keyInfo['from'] = None

            path = self._relativePath("/" + keyInfo['fullpath'])

            if path is None:
                continue

            diff = Store.Diff(self, keyInfo['to'], keyInfo['from'], key.size)

            logger.debug("Adding %s in %s", diff, path)

            self.diffs[diff.fromVol].append(diff)
            self.paths[diff.toVol].append(path)

        # logger.debug("Diffs:\n%s", pprint.pformat(self.diffs))
        # logger.debug("Vols:\n%s", pprint.pformat(self.vols))

    def listContents(self):
        """ Return list of volumes or diffs in this Store's selected directory. """
        return [diff for vol in self.diffs.values() for diff in vol]

    def getEdges(self, fromVol):
        """ Return the edges available from fromVol. """
        return self.diffs[fromVol]

    def hasEdge(self, diff):
        """ Test whether edge is in this sink. """
        return diff.toVol in [d.toVol for d in self.diffs[diff.fromVol]]

    def receive(self, diff, paths):
        """ Return Context Manager for a file-like (stream) object to store a diff. """
        path = self.selectReceivePath(paths)
        keyName = self._keyName(diff.toUUID, diff.fromUUID, path)

        if self._skipDryRun(logger)("receive %s in %s", keyName, self):
            return None

        return _Uploader(self.bucket, keyName)

    def receiveVolumeInfo(self, paths):
        """ Return Context Manager for a file-like (stream) object to store volume info. """
        path = self.selectReceivePath(paths)
        path = path + ".bs"

        if self._skipDryRun(logger)("receive info in '%s'", path):
            return None

        return io.BufferedWriter(_Uploader(self.bucket, path), buffer_size=theInfoBufferSize)

    theKeyPattern = "^(?P<fullpath>.*)/(?P<to>[-a-zA-Z0-9]*)_(?P<from>[-a-zA-Z0-9]*)$"

    def _keyName(self, toUUID, fromUUID, path):
        return "%s/%s_%s" % (path, toUUID, fromUUID)

    def _parseKeyName(self, name):
        """ Returns dict with fullpath, to, from. """
        if name.endswith(".bs"):
            return {'type': 'info'}

        match = self.keyPattern.match(name)
        if not match:
            return None

        match = match.groupdict()
        match.update(type='diff')

        return match

    def send(self, diff, streamContext, progress=True):
        """ Write the diff (toVol from fromVol) to the stream context manager. """
        path = self.getSendPath(diff.toVol)
        keyName = self._keyName(diff.toUUID, diff.fromUUID, path)
        key = self.bucket.get_key(keyName)

        if self._skipDryRun(logger)("send %s in %s", keyName, self):
            return

        with streamContext as stream:
            if progress:
                key.get_contents_to_file(stream, cb=_DisplayProgress(), num_cb=theProgressCount)
            else:
                key.get_contents_to_file(stream)

        if progress:
            sys.stdout.write("\n")


class _DisplayProgress:

    def __init__(self):
        self.startTime = datetime.datetime.now()

    def __call__(self, sent, total):
        if not sys.stdout.isatty():
            return

        elapsed = datetime.datetime.now() - self.startTime
        mbps = (sent * 8 / (10 ** 6) / elapsed.total_seconds())

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
        if exceptionType is not None:
            logger.error("abort")
        else:
            logger.debug("close")
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

    def writeable(self):
        return True

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
