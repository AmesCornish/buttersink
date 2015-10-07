""" Back-end for AWS S3.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

# Docs: http://boto.readthedocs.org/en/latest/

if True:  # Imports and constants
    if True:  # Imports
        from util import humanize
        import progress
        import Store
        import util

        import boto
        import boto.s3.connection
        import collections
        import io
        import logging
        import os.path
        import re
    if True:  # Constants
        # Maximum xumber of progress reports per chunk
        theProgressCount = 50

        # This does transparent S3 server-side encryption
        isEncrypted = True

        # Minimum chuck size is 5M.
        theInfoBufferSize = 10 * (1 << 20)

        logger = logging.getLogger(__name__)

        theTrashPrefix = "trash/"

# logger.setLevel('DEBUG')


def _displayTraceBack():
    return ""
    util.displayTraceBack()


class S3Store(Store.Store):

    """ An S3 bucket synchronization source or sink. """

    def __init__(self, host, path, mode, dryrun):
        """ Initialize.

        host is the bucket name.
        path is an object key prefix to use.

        """
        super(S3Store, self).__init__(host, "/" + path, mode, dryrun)

        self.bucketName = host

        self.keyPattern = re.compile(S3Store.theKeyPattern % ())

        # { fromVol: [diff] }
        self.diffs = None
        self.extraKeys = None

        logger.info("Listing %s contents...", self)

        try:
            # Orginary calling format returns a 301 without specifying a location.
            # Subdomain calling format does not require specifying the region.
            s3 = boto.s3.connection.S3Connection(
                # calling_format=boto.s3.connection.ProtocolIndependentOrdinaryCallingFormat(),
                calling_format=boto.s3.connection.SubdomainCallingFormat(),
                )
            # s3 = boto.connect_s3()   # Often fails with 301
            # s3 = boto.s3.connect_to_region('us-west-2')  # How would we know the region?
        except boto.exception.NoAuthHandlerFound:
            logger.error("Try putting S3 credentials into ~/.boto")
            raise

        self.bucket = s3.get_bucket(self.bucketName)
        self.isRemote = True

    def __unicode__(self):
        """ Return text description. """
        return u'S3 Bucket "%s"' % (self.bucketName)

    def __str__(self):
        """ Return text description. """
        return unicode(self).encode('utf-8')

    def _flushPartialUploads(self, dryrun):
        for upload in self.bucket.list_multipart_uploads():
            # logger.debug("Upload: %s", upload.__dict__)
            # for part in upload:
                # logger.debug("  Part: %s", part.__dict__)

            if not upload.key_name.startswith(self.userPath.lstrip("/")):
                continue

            if self._skipDryRun(logger, 'INFO', dryrun)(
                "Delete old partial upload: %s (%d parts)",
                upload,
                len([part for part in upload]),
            ):
                continue

            upload.cancel_upload()

    def _fillVolumesAndPaths(self, paths):
        """ Fill in paths.

        :arg paths: = { Store.Volume: ["linux path",]}
        """
        self.diffs = collections.defaultdict((lambda: []))
        self.extraKeys = {}

        for key in self.bucket.list():
            if key.name.startswith(theTrashPrefix):
                continue

            keyInfo = self._parseKeyName(key.name)

            if keyInfo is None:
                if key.name[-1:] != '/':
                    logger.warning("Ignoring '%s' in S3", key.name)
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
            paths[diff.toVol].append(path)

            self.extraKeys[diff] = path

        # logger.debug("Diffs:\n%s", pprint.pformat(self.diffs))
        # logger.debug("Vols:\n%s", pprint.pformat(self.vols))
        # logger.debug("Extra:\n%s", (self.extraKeys))

    def listContents(self):
        """ Return list of volumes or diffs in this Store's selected directory. """
        items = list(self.extraKeys.items())
        items.sort(key=lambda t: t[1])

        (count, size) = (0, 0)

        for (diff, path) in items:
            if path.startswith("/"):
                continue
            yield str(diff)
            count += 1
            size += diff.size

        yield "TOTAL: %d diffs %s" % (count, humanize(size))

    def getEdges(self, fromVol):
        """ Return the edges available from fromVol. """
        return self.diffs[fromVol]

    def hasEdge(self, diff):
        """ Test whether edge is in this sink. """
        return diff.toVol in [d.toVol for d in self.diffs[diff.fromVol]]

    def measureSize(self, diff, chunkSize):
        """ Spend some time to get an accurate size. """
        logger.warn("Don't need to measure S3 diffs")

    def receive(self, diff, paths):
        """ Return Context Manager for a file-like (stream) object to store a diff. """
        path = self.selectReceivePath(paths)
        keyName = self._keyName(diff.toUUID, diff.fromUUID, path)

        if self._skipDryRun(logger)("receive %s in %s", keyName, self):
            return None

        progress = _BotoProgress(diff.size) if self.showProgress is True else None
        return _Uploader(self.bucket, keyName, progress)

    def receiveVolumeInfo(self, paths):
        """ Return Context Manager for a file-like (stream) object to store volume info. """
        path = self.selectReceivePath(paths)
        path = path + Store.theInfoExtension

        if self._skipDryRun(logger)("receive info in '%s'", path):
            return None

        return _Uploader(self.bucket, path, bufferSize=theInfoBufferSize)

    theKeyPattern = "^(?P<fullpath>.*)/(?P<to>[-a-zA-Z0-9]*)_(?P<from>[-a-zA-Z0-9]*)$"

    def _keyName(self, toUUID, fromUUID, path):
        return "%s/%s_%s" % (self._fullPath(path).lstrip("/"), toUUID, fromUUID)

    def _parseKeyName(self, name):
        """ Returns dict with fullpath, to, from. """
        if name.endswith(Store.theInfoExtension):
            return {'type': 'info'}

        match = self.keyPattern.match(name)
        if not match:
            return None

        match = match.groupdict()
        match.update(type='diff')

        return match

    def send(self, diff):
        """ Write the diff (toVol from fromVol) to the stream context manager. """
        path = self._fullPath(self.extraKeys[diff])
        keyName = self._keyName(diff.toUUID, diff.fromUUID, path)
        key = self.bucket.get_key(keyName)

        if self._skipDryRun(logger)("send %s in %s", keyName, self):
            return None

        progress = _BotoProgress(diff.size) if self.showProgress is True else None
        return _Downloader(key, progress)

    def keep(self, diff):
        """ Mark this diff (or volume) to be kept in path. """
        path = self.extraKeys[diff]

        if not path.startswith("/"):
            logger.debug("Keeping %s", path)
            del self.extraKeys[diff]
            return

        # Copy into self.userPath, if not there already

        keyName = self._keyName(diff.toUUID, diff.fromUUID, path)
        newPath = os.path.join(self.userPath, os.path.basename(path))
        newName = self._keyName(diff.toUUID, diff.fromUUID, newPath)

        if not self._skipDryRun(logger)("Copy %s to %s", keyName, newName):
            self.bucket.copy_key(newName, self.bucket.name, keyName)

    def deleteUnused(self):
        """ Delete any old snapshots in path, if not kept. """
        (count, size) = (0, 0)

        for (diff, path) in self.extraKeys.items():
            if path.startswith("/"):
                continue

            keyName = self._keyName(diff.toUUID, diff.fromUUID, path)

            count += 1
            size += diff.size

            if self._skipDryRun(logger, 'INFO')("Trash: %s", diff):
                continue

            try:
                self.bucket.copy_key(theTrashPrefix + keyName, self.bucket.name, keyName)
                self.bucket.delete_key(keyName)
            except boto.exception.S3ResponseError as error:
                logger.error("%s: %s", error.code, error.message)

            try:
                keyName = os.path.dirname(keyName) + Store.theInfoExtension
                self.bucket.copy_key(theTrashPrefix + keyName, self.bucket.name, keyName)
                self.bucket.delete_key(keyName)
            except boto.exception.S3ResponseError as error:
                logger.debug("%s: %s", error.code, error.message)

        logger.info("Trashed %d diffs (%s)", count, humanize(size))

    def deletePartials(self):
        """ Delete any old partial uploads/downloads in path. """
        self._flushPartialUploads(self.dryrun)


class _BotoProgress(progress.DisplayProgress):

    def __init__(self, total=None, chunkName=None, parent=None):
        super(_BotoProgress, self).__init__(total, chunkName, parent)
        self.numCallBacks = theProgressCount

    def __call__(self, sent, total):
        logger.debug("BotoProgress: %d/%d", sent, total)
        self.total = total
        self.update(sent)

    @staticmethod
    def botoArgs(self):
        if self is not None:
            return {'cb': self, 'num_cb': self.numCallBacks}
        else:
            return {'cb': None, 'num_cb': None}


class _Downloader(io.RawIOBase):

    def __init__(self, key, progress=None):
        self.progress = progress
        self.key = key
        self.mark = 0

    def __enter__(self):
        if self.progress:
            self.progress.__enter__()
        return self

    def __exit__(self, exceptionType, exceptionValue, traceback):
        if self.progress:
            self.progress.__exit__(exceptionType, exceptionValue, traceback)

    def read(self, n=-1):
        if self.mark >= self.key.size or n == 0:
            return b''

        if n < 0:
            headers = None
        else:
            headers = {"Range": "bytes=%s-%s" % (self.mark, self.mark + n - 1)}

        if self.progress is None:
            data = self.key.get_contents_as_string(
                headers,
            )
        else:
            progress = _BotoProgress(n, "Range", self.progress)
            with progress:
                data = self.key.get_contents_as_string(
                    headers, **_BotoProgress.botoArgs(progress)
                )
        size = len(data)

        assert n < 0 or size <= n, (size, data[:10])

        self.mark += size

        return data

    def readable(self):
        return True


class _Uploader(io.RawIOBase):

    def __init__(self, bucket, keyName, progress=None, bufferSize=None):
        self.progress = progress
        self.bucket = bucket
        self.keyName = keyName.lstrip("/")
        self.uploader = None
        self.parts = util.DefaultList()
        self.chunkCount = None
        self.metadata = {}
        self.bufferedWriter = None
        self.bufferSize = bufferSize
        self.exception = None

    def __enter__(self):
        self.open()
        if self.progress:
            self.progress.__enter__()
        if self.bufferSize:
            self.bufferedWriter = io.BufferedWriter(self, buffer_size=self.bufferSize)
            return self.bufferedWriter
        else:
            return self

    def open(self):
        if self.uploader is not None:
            logger.warning(
                "Ignoring double open%s",
                _displayTraceBack(),
            )
            return

        logger.debug("Opening")

        self.chunkCount = 0
        self.parts = util.DefaultList()

        for upload in self.bucket.list_multipart_uploads():
            if upload.key_name != self.keyName:
                continue

            logger.debug("Upload: %s", upload.__dict__)
            for part in upload:
                logger.debug("  Part: %s", part.__dict__)
                self.parts[part.part_number - 1] = (part.size, part.etag)

            if not self.parts:
                continue

            self.uploader = upload
            return

        self.uploader = self.bucket.initiate_multipart_upload(
            self.keyName,
            encrypt_key=isEncrypted,
            metadata=self.metadata,
        )

        assert self.uploader is not None

    def __exit__(self, exceptionType, exceptionValue, traceback):
        self.exception = exceptionValue
        if self.bufferedWriter:
            self.bufferedWriter.close()
            if self.uploader is not None:
                logger.warn("BufferedWriter didn't close raw stream.")
        else:
            self.close()

        if self.progress:
            self.progress.__exit__(exceptionType, exceptionValue, traceback)

        return False  # Don't suppress exception

    def skipChunk(self, chunkSize, checkSum, data=None):
        part = self.parts[self.chunkCount]
        if part is None:
            return False

        (size, tag) = part
        tag = tag.strip('"')

        if size != chunkSize:
            logger.warning("Unexpected chunk size %d instead of %d", chunkSize, size)
            # return False
        if tag != checkSum:
            logger.warning("Bad check sum %d instead of %d", checkSum, tag)
            return False

        self.chunkCount += 1

        logger.info(
            "Skipping already uploaded %s chunk #%d",
            humanize(chunkSize),
            self.chunkCount,
        )

        return True

    def write(self, bytes):
        if len(bytes) == 0:
            logger.debug("Ignoring empty upload request.")
            return 0
        return self.upload(bytes)

    def close(self):
        if self.uploader is None:
            logger.debug(
                "Ignoring double close%s",
                _displayTraceBack(),
            )
            return
        logger.debug(
            "Closing%s",
            _displayTraceBack(),
        )

        if self.exception is None:
            self.uploader.complete_upload()
            # You cannot change metadata after uploading
            # if self.metadata:
            #     key = self.bucket.get_key(self.keyName)
            #     key.update_metadata(self.metadata)
        else:
            # self.uploader.cancel_upload()   # Delete (most of) the uploaded chunks.
            parts = [part for part in self.uploader]
            logger.debug("Uploaded parts: %s", parts)
            pass  # Leave unfinished upload to resume later

        self.uploader = None

    def fileno(self):
        raise IOError("S3 uploads don't use file numbers.")

    def writable(self):
        return True

    def upload(self, bytes):
        if self.chunkCount is None:
            raise Exception("Uploading before opening uploader.")

        self.chunkCount += 1
        size = len(bytes)
        fileObject = io.BytesIO(bytes)

        if self.progress is None:
            self.uploader.upload_part_from_file(
                fileObject, self.chunkCount,
            )
        else:
            cb = _BotoProgress(
                size,
                "Chunk #%d" % (self.chunkCount),
                self.progress,
            )

            with cb:
                self.uploader.upload_part_from_file(
                    fileObject, self.chunkCount, **_BotoProgress.botoArgs(cb)
                )

        return size
