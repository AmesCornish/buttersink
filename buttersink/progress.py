""" Display Mbs progress on tty. """

from __future__ import division

import util

import datetime
import sys


class DisplayProgress(object):

    """ Class to display Mbs progress on tty. """

    def __init__(self, total=None, chunkName=None, parent=None, suppress=False):
        """ Initialize. """
        self.startTime = None
        self.offset = None
        self.total = total
        self.name = chunkName
        self.parent = parent
        self.suppress = suppress or not sys.stdout.isatty()

    def __enter__(self):
        """ For with statement. """
        self.startTime = datetime.datetime.now()
        self.offset = 0
        return self

    def __exit__(self, exceptionType, exceptionValue, traceback):
        """ For with statement. """
        self.close()
        return False

    def update(self, sent):
        """ Update self and parent with intermediate progress. """
        self.offset = sent

        now = datetime.datetime.now()

        elapsed = now - self.startTime
        mbps = (sent * 8 / (10 ** 6) / elapsed.total_seconds())

        self._display(sent, now, self.name, mbps)

    def _display(self, sent, now, chunk, mbps):
        """ Display intermediate progress. """
        if self.parent is not None:
            self.parent._display(self.parent.offset + sent, now, chunk, mbps)
            return

        if self.suppress:
            return

        elapsed = now - self.startTime

        if sent > 0 and self.total is not None:
            eta = (self.total - sent) * elapsed.total_seconds() / sent
            eta = datetime.timedelta(seconds=eta)
        else:
            eta = None

        sys.stdout.write(
            "\r %s: Sent %s of %s%s (%s %.3g Mbps) ETA: %s %20s\r" % (
                elapsed,
                util.humanize(sent),
                util.humanize(self.total),
                "--%" if self.total is None else " (%d%%)" % (int(100 * sent / self.total),),
                chunk,
                mbps,
                eta,
                " ",
            )
        )

        sys.stdout.flush()

    def close(self):
        """ Stop overwriting display, or update parent. """
        if self.parent:
            self.parent.update(self.parent.offset + self.offset)
            return
        if self.suppress:
            return
        sys.stdout.write("\n")
