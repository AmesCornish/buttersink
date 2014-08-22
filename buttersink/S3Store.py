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
        import re
        import sys
    if True:  # Constants
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
        self.extraKeys = None

        logger.info("Listing %s contents...", self)

        try:
            s3 = boto.connect_s3()
        except boto.exception.NoAuthHandlerFound:
            logger.error("Try putting S3 credentials into ~/.boto")
            raise

        self.bucket = s3.get_bucket(self.bucketName)
        self._flushPartialUploads(True)
        self._fillVolumesAndPaths()

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

            if self._skipDryRun(logger, dryrun)(
                "%s old partial upload: %s (%d parts)",
                "Found" if dryrun else "Canceling",
                upload,
                len([part for part in upload]),
            ):
                return

            upload.cancel_upload()

    def _fillVolumesAndPaths(self):
        """ Fill in self.paths. """
        self.diffs = collections.defaultdict((lambda: []))
        self.extraKeys = {}

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

            self.extraKeys[diff] = path

        # logger.debug("Diffs:\n%s", pprint.pformat(self.diffs))
        # logger.debug("Vols:\n%s", pprint.pformat(self.vols))
        logger.debug("Extra:\n%s", (self.extraKeys))

    def listContents(self):
        """ Return list of volumes or diffs in this Store's selected directory. """
        items = list(self.extraKeys.items())
        items.sort(key=lambda t: t[1])
        return [str(diff) for (diff, path) in items if not path.startswith("/")]

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
        return "%s/%s_%s" % (self._fullPath(path).lstrip("/"), toUUID, fromUUID)

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

    def send(self, diff, progress=True):
        """ Write the diff (toVol from fromVol) to the stream context manager. """
        path = self.getSendPath(diff.toVol)
        keyName = self._keyName(diff.toUUID, diff.fromUUID, path)
        key = self.bucket.get_key(keyName)

        if self._skipDryRun(logger)("send %s in %s", keyName, self):
            return None

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
        for (diff, path) in self.extraKeys.items():
            if path.startswith("/"):
                continue

            keyName = self._keyName(diff.toUUID, diff.fromUUID, path)

            if self._skipDryRun(logger)("Put %s into trash", keyName):
                continue

            self.bucket.copy_key("trash/" + keyName, self.bucket.name, keyName)
            self.bucket.delete_key(keyName)

    def deletePartials(self):
        """ Delete any old partial uploads/downloads in path. """
        self._flushPartialUploads(self.dryrun)


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


class _Downloader(io.RawIOBase):

    def __init__(self, key, progress=True):
        self.key = key
        self.progress = progress
        self.mark = 0

    def __enter__(self):
        return self

    def __exit__(self, exceptionType, exceptionValue, traceback):
        if self.progress:
            sys.stdout.write("\n")

    def read(self, n=-1):
        if self.mark >= self.key.size:
            return b''

        if self.progress:
            (cb, num_cb) = (_DisplayProgress(), theProgressCount)
        else:
            (cb, num_cb) = (None, None)

        if n < 0:
            n = Store.theChunkSize

        headers = {"Range": "bytes=%s-%s" % (self.mark, self.mark + n - 1)}

        # TODO: Fix progress indicator by using a resumable download handler

        data = self.key.get_contents_as_string(headers, cb=cb, num_cb=num_cb)

        assert len(data) <= n, (len(data), data[:10])

        self.mark += len(data)

        return data

    def readable(self):
        return True


class DefaultList(list):

    """ list that automatically inserts None for missing items. """

    def __setitem__(self, index, value):
        """ Set item. """
        if len(self) > index:
            return list.__setitem__(self, index, value)
        if len(self) < index:
            self.extend([None] * (index - len(self)))
        list.append(self, value)

    def __getitem__(self, index):
        """ Set item. """
        if index >= len(self):
            return None
        return list.__getitem__(self, index)


class _Uploader(io.RawIOBase):

    def __init__(self, bucket, keyName):
        self.bucket = bucket
        self.keyName = keyName.lstrip("/")
        self.uploader = None
        self.parts = DefaultList()
        self.chunkCount = None
        self.metadata = {}
        self.progress = True

    def __enter__(self):
        self.open()
        return self

    def open(self):
        self.chunkCount = 0
        self.parts = DefaultList()

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

    def __exit__(self, exceptionType, exceptionValue, traceback):
        self.close(abort=exceptionType is not None)
        if exceptionType is not None:
            logger.error("abort")
        else:
            logger.debug("close")
        return False  # Don't supress exception

    def skipChunk(self, chunkSize, checkSum, data=None):
        part = self.parts[self.chunkCount]
        if part is None:
            return False

        (size, tag) = part
        tag = tag.strip('"')

        if size != chunkSize:
            logger.warn("Unexpected chunk size %d instead of %d", chunkSize, size)
            # return False
        if tag != checkSum:
            logger.warn("Bad check sum %d instead of %d", checkSum, tag)
            return False

        self.chunkCount += 1

        logger.info(
            "Skipping already uploaded %s chunk #%d",
            Store.humanize(chunkSize),
            self.chunkCount,
        )

        return True

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
