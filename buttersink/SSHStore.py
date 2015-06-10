""" Manage read-only snapshots in a remote filesystem over SSH.

Copyright (c) 2014-2015 Ames Cornish.  All rights reserved.  Licensed under GPLv3.
"""

from progress import DisplayProgress
import ButterStore
import Store
import version

import io
import json
import logging
import os.path
import platform
import subprocess
import sys
import traceback
import urllib

logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')

DEVNULL = open(os.devnull, 'wb')

theVersion = version.version


class _Obj2Arg:

    def vol(self, vol):
        return 'None' if vol is None else vol.uuid

    def diff(self, diff):
        if diff is None:
            return ('None', 'None')
        else:
            return (self.vol(diff.toVol), self.vol(diff.fromVol), )


class _Arg2Obj:

    def __init__(self, store):
        self.sink = store

    def vol(self, uuid):
        return None if uuid == 'None' else Store.Volume(uuid, None)

    def diff(self, toUUID, fromUUID, estimatedSize='None'):
        if estimatedSize == 'None':
            return Store.Diff(self.sink, self.vol(toUUID), self.vol(fromUUID))
        else:
            estimatedSize = int(float(estimatedSize))
            return Store.Diff(self.sink, self.vol(toUUID), self.vol(fromUUID), estimatedSize, True)

    def bool(self, text):
        return text.lower() in ['true', 'yes', 'on', '1', 't', 'y']


class _Obj2Dict:

    """ Serialize to dictionary. """

    def vol(self, vol):
        if vol is None:
            return None
        return dict(
            uuid=vol.uuid,
            gen=vol.gen,
            size=vol.size,
            exclusiveSize=vol.exclusiveSize,
        )

    def diff(self, diff):
        """ Serialize to a dictionary. """
        if diff is None:
            return None
        return dict(
            toVol=diff.toUUID,
            fromVol=diff.fromUUID,
            size=diff.size,
            sizeIsEstimated=diff.sizeIsEstimated,
        )


class _Dict2Obj:

    def __init__(self, store):
        self.sink = store

    def vol(self, values):
        return Store.Volume(**values)

    def diff(self, values):
        return Store.Diff(sink=self.sink, **values)


class _SSHStream(io.RawIOBase):

    def __init__(self, client, progress=None):
        self._client = client
        self._open = True
        self._progress = progress
        self.totalSize = 0

    def __enter__(self):
        if self._progress:
            self._progress.__enter__()
        return self

    def __exit__(self, exceptionType, exception, trace):
        if self._progress:
            self._progress.__exit__(exceptionType, exception, trace)

        if not self._open:
            return False

        try:
            result = self._client.streamWrite(0)
            self._open = False
        except Exception as error:
            if exceptionType is None:
                raise
            else:
                logger.debug("Secondary error: %s", error)

        if exceptionType is None and result and 'error' in result:
            raise Exception(result)

        return False  # Don't supress exception

    def write(self, data):
        size = len(data)

        if size == 0:
            return

        try:
            result = self._client.streamWrite(size)

            if self._progress:
                self._progress.update(self.totalSize)

            if result.get('stream', False):
                self._client._process.stdin.write(data)
                self.totalSize += size
                if self._progress:
                    self._progress.update(self.totalSize)

                result = self._client._getResult()
        except Exception as error:
            self._client.error = error  # Don't try writing to this client again
            raise

        if result and 'error' in result:
            raise Exception(result)

    def read(self, size):
        if size == 0:
            return ''

        try:
            result = self._client.streamRead(size)

            size = result['size']
            if size == 0:
                self._open = False
                return ''

            if self._progress:
                self._progress.update(self.totalSize)
            data = self._client._process.stdout.read(size)
            self.totalSize += size
            if self._progress:
                self._progress.update(self.totalSize)

            result = self._client._getResult()
        except Exception as error:
            self._client.error = error  # Don't try reading from this client again
            raise

        if result and 'error' in result:
            raise Exception(result)

        return data


class SSHStore(Store.Store):

    """ A synchronization source or sink to a btrfs over SSH. """

    def __init__(self, host, path, mode, dryrun):
        """ Initialize.

        :arg host:   ssh host.
        """
        # For simplicity, only allow absolute paths
        # Don't lose a trailing slash -- it's significant
        path = "/" + os.path.normpath(path) + ("/" if path.endswith("/") else "")

        super(SSHStore, self).__init__(host, path, mode, dryrun)

        self.host = host
        self._client = _Client(host, 'r' if dryrun else mode, path)
        self.isRemote = True

        self.toArg = _Obj2Arg()
        self.toObj = _Dict2Obj(self)

    def __unicode__(self):
        """ English description of self. """
        return u"ssh://%s%s" % (self.host, self.userPath)

    def _open(self):
        """ Open connection to remote host. """
        self._client._open()

    def _close(self):
        """ Close connection to remote host. """
        self._client._close()

    # Abstract methods

    def _fillVolumesAndPaths(self, paths):
        """ Fill in paths.

        :arg paths: = { Store.Volume: ["linux path",]}
        """
        for (volDict, volPaths) in self._client.fillVolumesAndPaths():
            vol = Store.Volume(**volDict)
            paths[vol] = volPaths

    # Abstract methods

    def getEdges(self, fromVol):
        """ Return the edges available from fromVol. """
        return [
            self.toObj.diff(diff)
            for diff in self._client.getEdges(self.toArg.vol(fromVol))
        ]

    def measureSize(self, diff, chunkSize):
        """ Spend some time to get an accurate size. """
        (toUUID, fromUUID) = self.toArg.diff(diff)
        isInteractive = sys.stderr.isatty()
        return self.toObj.diff(self._client.measureSize(
            toUUID,
            fromUUID,
            diff.size,
            chunkSize,
            isInteractive,
        ))

    def hasEdge(self, diff):
        """ True if Store already contains this edge. """
        return diff.toVol in self.paths

    def send(self, diff):
        """ Return Context Manager for a file-like (stream) object to send a diff. """
        if Store.skipDryRun(logger, self.dryrun)("send %s", diff):
            return None

        (diffTo, diffFrom) = self.toArg.diff(diff)
        self._client.send(diffTo, diffFrom)

        progress = DisplayProgress(diff.size) if self.showProgress is True else None
        return _SSHStream(self._client, progress)

    def receive(self, diff, paths):
        """ Return Context Manager for a file-like (stream) object to store a diff. """
        path = self.selectReceivePath(paths)
        path = self._relativePath(path)

        if Store.skipDryRun(logger, self.dryrun)("receive to %s", path):
            return None

        (diffTo, diffFrom) = self.toArg.diff(diff)
        self._client.receive(path, diffTo, diffFrom)

        progress = DisplayProgress(diff.size) if self.showProgress is True else None
        return _SSHStream(self._client, progress)

    def receiveVolumeInfo(self, paths):
        """ Return Context Manager for a file-like (stream) object to store volume info. """
        path = self.selectReceivePath(paths)
        path = path + Store.theInfoExtension

        if Store.skipDryRun(logger, self.dryrun)("receive info to %s", path):
            return None

        self._client.receiveInfo(path)

        return _SSHStream(self._client)

    def keep(self, diff):
        """ Mark this diff (or volume) to be kept in path. """
        (toUUID, fromUUID) = self.toArg.diff(diff)
        self._client.keep(toUUID, fromUUID)
        logger.debug("Kept %s", diff)

    def deleteUnused(self):
        """ Delete any old snapshots in path, if not kept. """
        if self.dryrun:
            self._client.listUnused()
        else:
            self._client.deleteUnused()

    def deletePartials(self):
        """ Delete any old partial uploads/downloads in path. """
        if self.dryrun:
            self._client.listPartials()
        else:
            self._client.deletePartials()


class _Client(object):

    def __init__(self, host, mode, directory):
        self._host = host
        self._mode = mode
        self._directory = urllib.quote_plus(directory, '/')
        self._process = None
        self.error = None

    def _open(self):
        """ Open connection to remote host. """
        if self._process is not None:
            return

        cmd = [
            'ssh',
            self._host,
            'sudo',
            'buttersink',
            '--server',
            '--mode',
            self._mode,
            self._directory
        ]
        logger.debug("Connecting with: %s", cmd)
        self._process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stderr=sys.stderr,
            # stdout=sys.stdout,
            stdout=subprocess.PIPE,
        )

        version = self.version()
        logger.info("Remote version: %s", version)

    def _close(self):
        """ Close connection to remote host. """
        if self._process is None:
            return

        self.quit()

        self._process.stdin.close()

        logger.debug("Waiting for ssh process to finish...")
        self._process.wait()  # Wait for ssh session to finish.

        # self._process.terminate()
        # self._process.kill()

        self._process = None

    def _checkMode(self, name, mode):
        modes = ["read-only", "append", "write"]

        def value(mode):
            return [s[0] for s in modes].index(mode)

        allowedMode = value(self._mode)
        requestedMode = value(mode)
        if requestedMode > allowedMode:
            raise Exception(
                "%s connection does not allow %s method '%s'" %
                (modes[allowedMode], modes[requestedMode], name)
            )

    def _getResult(self):
        result = self._process.stdout.readline().rstrip("\n")
        # logger.debug('Result: %s', result)
        try:
            result = json.loads(result)
        except:
            # result += os.read(self._process.stdout.fileno(), 5)
            # result += self._process.stdout.read()
            logger.error(result)
            raise Exception("Fatal remote ssh server error")
        return result

    def _sendCommand(self, *command):
        if self.error is not None:
            # logger.warn("Not sending %s because of %s", command, self.error)
            return dict(error=self.error, message="Can't send command", command=command[0])

        try:
            command = ['None' if c is None else urllib.quote_plus(str(c), '/') for c in command]
        except Exception:
            raise Exception("Can't send '%s' over ssh." % (command,))

        # logger.debug("Command line: %s", command)
        commandLine = " ".join(command) + "\n"

        try:
            self._process.stdin.write(commandLine)

            result = self._getResult()
        except Exception as error:
            self.error = error
            raise

        if result and 'error' in result:
            raise Exception(result)

        return result

    @classmethod
    def _addMethod(cls, method, name, mode):
        def fn(self, *args):
            self._checkMode(name, mode)
            return self._sendCommand(name, *args)
        setattr(cls, method, fn)

commands = {}


def command(name, mode):
    """ Label a method as a command with name. """
    def decorator(fn):
        commands[name] = fn.__name__
        _Client._addMethod(fn.__name__, name, mode)
        return fn
    return decorator


class StoreProxyServer(object):

    """ Runs a ButterStore and responds to queries over ssh.

    Use in a 'with' statement.
    """

    def __init__(self, path, mode):
        """ Initialize. """
        logger.debug("Proxy(%s) %s", mode, path)
        self.path = path
        self.mode = mode
        self.butterStore = None
        self.running = False
        self.toObj = None
        self.toDict = None
        self.stream = None

    def __enter__(self):
        """ Enter 'with' statement. """
        return self

    def __exit__(self, exceptionType, exception, trace):
        """ Exit 'with' statement. """
        try:
            self._close()
        except Exception as error:
            if exceptionType is None:
                raise
            else:
                logger.info("Error on close: %s", error)
        return False

    def _open(self, stream):
        if self.stream is not None:
            logger.warn("%s not closed.", self.stream)
        self._close()
        self.stream = stream
        self.stream.__enter__()

    def _close(self):
        if self.stream is None:
            return
        try:
            self.stream.__exit__(None, None, None)
        finally:
            self.stream = None

    def run(self):
        """ Run the server.  Returns with system error code. """
        normalized = os.path.normpath(self.path) + ("/" if self.path.endswith("/") else "")
        if self.path != normalized:
            sys.stderr.write("Please use full path '%s'" % (normalized,))
            return -1

        self.butterStore = ButterStore.ButterStore(None, self.path, self.mode, dryrun=False)
        # self.butterStore.ignoreExtraVolumes = True

        self.toObj = _Arg2Obj(self.butterStore)
        self.toDict = _Obj2Dict()

        self.running = True

        with self.butterStore:
            with self:
                while self.running:
                    self._processCommand()

        return 0

    def _errorInfo(self, command, error):
        trace = traceback.format_exc()
        trace = trace.splitlines()[-3]

        return dict(
            error=str(error),
            errorType=type(error).__name__,
            command=command,
            server=True,
            traceback=trace,
            )

    def _sendResult(self, result):
        """ Send parseable json result of command. """
        # logger.debug("Result: %s", result)

        try:
            result = json.dumps(result)
        except Exception as error:
            result = json.dumps(self._errorInfo(command, error))

        sys.stdout.write(result)
        sys.stdout.write("\n")
        sys.stdout.flush()

    def _errorMessage(self, message):
        sys.stderr.write(str(message))
        sys.stderr.write("\n")
        sys.stderr.flush()

    def _processCommand(self):
        commandLine = sys.stdin.readline().rstrip('\n').split(" ")
        commandLine = [urllib.unquote_plus(c) for c in commandLine]
        command = commandLine[0]

        # logger.debug("Command: '%s'", commandLine)

        if not command:
            self._errorMessage("No more commands -- terminating.")
            self.running = False
            return

        try:
            if command not in commands:
                raise Exception("Unknown command")

            method = getattr(self, commands[command])
            fn = method.__get__(self, StoreProxyServer)
            result = fn(*commandLine[1:])
            if result is None:
                result = dict(command=command, success=True)
        except Exception as error:
            # logger.exception("Failed %s", command)
            result = self._errorInfo(command, error)

        self._sendResult(result)

    @command('quit', 'r')
    def quit(self):
        """ Quit the server. """
        self.running = False
        return dict(message="Quitting")

    @command('version', 'r')
    def version(self):
        """ Return kernel and btrfs version. """
        return dict(
            buttersink=theVersion,
            btrfs=self.butterStore.butter.btrfsVersion,
            linux=platform.platform(),
        )

    @command('send', 'r')
    def send(self, diffTo, diffFrom):
        """ Do a btrfs send. """
        diff = self.toObj.diff(diffTo, diffFrom)
        self._open(self.butterStore.send(diff))

    @command('receive', 'a')
    def receive(self, path, diffTo, diffFrom):
        """ Receive a btrfs diff. """
        diff = self.toObj.diff(diffTo, diffFrom)
        self._open(self.butterStore.receive(diff, [path, ]))

    @command('write', 'r')
    def streamWrite(self, size):
        """ Send or receive a chunk of data.

        :arg size:  Amount of data.  0 indicates EOT.
        """
        size = int(size)
        if size == 0:
            self._close()
            return

        self._sendResult(dict(message="writing...", stream=True, size=size))
        data = sys.stdin.read(size)
        self.stream.write(data)

    @command('read', 'r')
    def streamRead(self, size):
        """ Send or receive a chunk of data.

        :arg size:  Amount of data requested.
        """
        size = int(size)
        data = self.stream.read(size)
        size = len(data)
        if size == 0:
            self._close()
            return dict(message="Finished", size=0)

        self._sendResult(dict(message="reading...", stream=True, size=size))
        sys.stdout.write(data)

    @command('volumes', 'r')
    def fillVolumesAndPaths(self):
        """ Get all volumes for initialization. """
        return [
            (self.toDict.vol(vol), paths)
            for vol, paths in self.butterStore.paths.items()
        ]

    @command('edges', 'r')
    def getEdges(self, fromVol):
        """ Return the edges available from fromVol. """
        return [self.toDict.diff(d) for d in self.butterStore.getEdges(self.toObj.vol(fromVol))]

    @command('measure', 'r')
    def measureSize(self, diffTo, diffFrom, estimatedSize, chunkSize, isInteractive):
        """ Spend some time to get an accurate size. """
        diff = self.toObj.diff(diffTo, diffFrom, estimatedSize)
        isInteractive = self.toObj.bool(isInteractive)
        self.butterStore.showProgress = None if isInteractive else False
        self.butterStore.measureSize(diff, int(chunkSize))
        return self.toDict.diff(diff)

    @command('keep', 'r')
    def keep(self, diffTo, diffFrom):
        """ Mark this diff (or volume) to be kept in path. """
        diff = self.toObj.diff(diffTo, diffFrom)
        self.butterStore.keep(diff)

    @command('delete', 'w')
    def deleteUnused(self):
        """ Delete any old snapshots in path, if not kept. """
        self.butterStore.deleteUnused()

    @command('clean', 'w')
    def deletePartials(self):
        """ Delete any old partial uploads/downloads in path. """
        self.butterStore.deletePartials()

    @command('listDelete', 'r')
    def listUnused(self):
        """ Delete any old snapshots in path, if not kept. """
        self.butterStore.deleteUnused(dryrun=True)

    @command('listClean', 'r')
    def listPartials(self):
        """ Delete any old partial uploads/downloads in path. """
        self.butterStore.deletePartials(dryrun=True)

    @command('info', 'a')
    def receiveInfo(self, path):
        """ Receive volume info. """
        self.stream = open(path, "w")
