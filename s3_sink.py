""" Back-end for AWS S3. """

import boto

import re

import logging
logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')


class S3Sink:

    """ An S3 bucket synchronization source or sink. """

    def __init__(self, host, path):
        """ Initialize.

        host is the bucket name.
        path is an object key prefix to use.
        """
        self.bucket = host
        self.path = path
        self.diffs = None
        self.vols = None
        self._listBucket()

    def _listBucket(self):
        s3 = boto.connect_s3()
        bucket = s3.get_bucket(self.bucket)
        pattern = re.compile("^(?P<to>[^/]*)/(?P<from>.*)$")

        logger.info("Listing bucket '%s' contents...", self.bucket)
        self.diffs = []
        for key in bucket.list(prefix=self.path):
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
