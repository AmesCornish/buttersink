""" Display Mbs progress on tty. """

from __future__ import division

import util

import datetime
import sys


class DisplayProgress(object):

    """ Class to display Mbs progress on tty. """

    def __init__(self, total=None, chunkName=None, parent=None):
        """ Initialize. """
        self.startTime = None
        self.offset = None
        self.total = total
        self.name = chunkName
        self.parent = parent
        self.output = sys.stderr

    def __enter__(self):
        """ For with statement. """
        self.open()

    def open(self):
        """ Reset time and counts. """
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

        elapsed = (now - self.startTime).total_seconds()
        if elapsed > 0:
            mbps = (sent * 8 / (10 ** 6)) / elapsed
        else:
            mbps = None

        self._display(sent, now, self.name, mbps)

    def _display(self, sent, now, chunk, mbps):
        """ Display intermediate progress. """
        if self.parent is not None:
            self.parent._display(self.parent.offset + sent, now, chunk, mbps)
            return

        elapsed = now - self.startTime

        if sent > 0 and self.total is not None and sent <= self.total:
            eta = (self.total - sent) * elapsed.total_seconds() / sent
            eta = datetime.timedelta(seconds=eta)
        else:
            eta = None

        self.output.write(
            "\r %s: Sent %s%s%s ETA: %s (%s) %s%20s\r" % (
                elapsed,
                util.humanize(sent),
                "" if self.total is None else " of %s" % (util.humanize(self.total),),
                "" if self.total is None else " (%d%%)" % (int(100 * sent / self.total),),
                eta,
                "" if not mbps else "%.3g Mbps " % (mbps,),
                chunk or "",
                " ",
            )
        )

        self.output.flush()

    def close(self):
        """ Stop overwriting display, or update parent. """
        if self.parent:
            self.parent.update(self.parent.offset + self.offset)
            return
        self.output.write("\n")
        self.output.flush()
