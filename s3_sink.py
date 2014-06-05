""" Back-end for AWS S3. """

# Docs: http://boto.readthedocs.org/en/latest/

import boto

import re

import logging
logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')


import io


class S3Sink:

    """ An S3 bucket synchronization source or sink. """

    def __init__(self, host, path):
        """ Initialize.

        host is the bucket name.
        path is an object key prefix to use.
        """
        self.bucketName = host

        s3 = boto.connect_s3()
        self.bucket = s3.get_bucket(self.bucketName)

        self.prefix = path
        self.diffs = None
        self.vols = None
        self._listBucket()

    def _listBucket(self):
        pattern = re.compile("^(?P<to>[^/]*)/(?P<from>.*)$")

        logger.info("Listing bucket '%s' contents...", self.bucketName)
        self.diffs = []
        for key in self.bucket.list(prefix=self.prefix):
            diff = pattern.match(key.name).groupdict()

            self.diffs.append(
                {'from': diff['from'], 'to': diff['to'], 'size': key.size})

        logger.debug(self.diffs)

        self.vols = {diff['to'] for diff in self.diffs}

    def listVolumes(self):
        """ Return list of volumes that are available. """
        return self.vols

    def iterEdges(self, fromVol):
        """ Return the edges available from fromVol.

        Returned edge is a dict: 'to' UUID, estimated 'size' in MB
        """
        for diff in self.diffs:
            if diff['from'] == fromVol:
                yield {'to': diff['to'], 'size': diff['size']}

    def receive(self, diff):
        """ Send diff to S3. """
        stream = diff.diffSink.send(diff)

        name = "%s%s/%s" % (self.prefix, diff.uuid, diff.previous)

        self._upload(stream, name)

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

# For S3 uploads
theChunkSize = 100 * 2**20

# This does transparent S3 server-side encryption
isEncrypted = True

# TODO: Get a real version number
theBtrfsVersion = '0.0'


def _displayProgress(sent, total):
    logger.info("Sent %d bytes of %d", sent, total)


class _Uploader:
    def __init__(self, bucket, keyName):
        self.bucket = bucket
        self.keyName = keyName
        self.upload = None
        self.chunkCount = None

    def __enter__(self):
        logger.info("Beginning upload to %s", self.keyName)
        self.upload = self.bucket.initiate_multipart_upload(
            self.keyName,
            encrypt_key=isEncrypted,
            metadata={'btrfsVersion': theBtrfsVersion},
            )
        self.chunkCount = 0
        return self

    def __exit__(self, exceptionType, exceptionValue, traceback):
        if exceptionType is None:
            self.upload.complete_upload()
        else:
            # TODO: this doesn't free storage used by part uploads currently in progress
            self.upload.cancel_upload()
        self.upload = None
        return False  # Don't supress exception

    def upload(self, bytes):
        self.chunkCount += 1
        logger.info("Uploading chunk #%d for %s", self.chunkCount, self.keyName)
        fileObject = io.BytesIO(bytes)
        self.upload.upload_part_from_file(fileObject, self.chunkCount)
