""" Utilities for working with fnctl.ioctl. """

# See <linux/ioctl.h> for C source.

import array
import collections
import fcntl
import itertools
import logging
import os
import struct

logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')

# constant for linux portability
NRBITS = 8
TYPEBITS = 8

# architecture specific
SIZEBITS = 14
DIRBITS = 2

NRMASK = (1 << NRBITS) - 1
TYPEMASK = (1 << TYPEBITS) - 1
SIZEMASK = (1 << SIZEBITS) - 1
DIRMASK = (1 << DIRBITS) - 1

NRSHIFT = 0
TYPESHIFT = NRSHIFT + NRBITS
SIZESHIFT = TYPESHIFT + TYPEBITS
DIRSHIFT = SIZESHIFT + SIZEBITS

NONE = 0
WRITE = 1
READ = 2


class t:

    """ Type definitions for translating linux C headers to Python struct format values. """

    (s8, s16, s32, s64) = 'bhlq'
    (u8, u16, u32, u64) = 'BHLQ'
    (le16, le32, le64) = (u16, u32, u64)  # Works on Linux x86

    char = 'c'

    max_u32 = (1 << 32) - 1
    max_u64 = (1 << 64) - 1

    @staticmethod
    def writeChar(value):
        """ Write a single-character string as a one-byte (u8) number. """
        return 0 if value is None else ord(value[0])

    @staticmethod
    def writeString(data):
        """ Write a string as null-terminated c string (bytes). """
        if data is None:
            return chr(0)

        return data.encode('utf-8') + chr(0)

    @staticmethod
    def readString(data):
        """ Read a null-terminated (c) string. """
        # CAUTION: Great for strings, horrible for buffers!
        return data.decode('utf-8').partition(chr(0))[0]

    @staticmethod
    def readBuffer(data):
        """ Trim zero bytes in buffer. """
        # CAUTION: Great for strings, horrible for buffers!
        return data.rstrip(chr(0))


def unzip(listOfLists):
    """ Inverse of zip to split lists. """
    return zip(*listOfLists)


class _SkipType:

    def popValue(self, argList):
        return None

    def yieldArgs(self, arg):
        if False:
            yield None  # Make this a generator


class _TypeWriter:

    def __init__(self, default, reader=None, writer=None):
        self._default = default
        self._writer = writer or (lambda x: x)
        self._reader = reader or (lambda x: x)

    def popValue(self, argList):
        return self._reader(argList.pop())

    def yieldArgs(self, arg):
        yield self._writer(arg) or self._default


class Structure:

    """ Model a C struct.

    Encapsulates a struct format with named item values.

    structure fields are (typeDef, name, len=1) arguments.
    typeDef can be a string or a structure itself.

    Example Structures:
        >>> s1 = Structure((t.char, 'char1'))
        >>> s2 = Structure(
        ... (t.u16, 'foo'),
        ... (t.u8, 'bar', 8, t.readString, t.writeString),
        ... (s1, 'foobar'),
        ... )
        >>> s2.fmt
        'H8sc'
        >>> s2.size
        11

    Instance variables:
        >>> s2._Tuple(1,2,3).__dict__
        OrderedDict([('foo', 1), ('bar', 2), ('foobar', 3)])
        >>> s2._packed
        True
        >>> s2._types.keys()
        ['foo', 'bar', 'foobar']

    Using a Structure:
        >>> myValues = dict(foo=8, bar=u"hola", foobar=dict(char1='a'))
        >>> data = s2.write(myValues)
        >>> data
        array('B', [8, 0, 104, 111, 108, 97, 0, 0, 0, 0, 97])
        >>> values = s2.read(data)
        >>> values
        StructureTuple(foo=8, bar=u'hola', foobar=StructureTuple(char1='a'))
        >>> values.foo
        8
        >>> values.foobar.char1
        'a'

    """

    def __init__(self, *fields, **keyArgs):
        """ Initialize. """
        (names, formats, types) = unzip([self._parseDefinition(*f) for f in fields])

        self._Tuple = collections.namedtuple("StructureTuple", names)

        self._fmt = "".join(formats)
        self._packed = keyArgs.get('packed', True)
        self._struct = struct.Struct("=" + self._fmt if self._packed else self._fmt)

        self._types = collections.OrderedDict(zip(names, types))

    @property
    def size(self):
        """ Total packed data size. """
        return self._struct.size

    @property
    def fmt(self):
        """ struct module format string without the leading byte-order character. """
        return self._fmt

    # This produces a dictionary of { fmtChar: defaultValue }
    defaults = dict(itertools.chain(*[
        [(fmtChar, defaultValue) for fmtChar in s]
        for (s, defaultValue) in [
            ("sp", ""),
            ("bBhHiIlLqQfdP", 0),
            ("?", False),
            ("c", chr(0)),
        ]]))

    skipType = _SkipType()

    @staticmethod
    def _parseDefinition(typeDef, name, len=1, reader=None, writer=None):
        """ Return (name, format, type) for field.

        type.popValue() and type.yieldArgs() must be implemented.

        """
        if isinstance(typeDef, Structure):
            return (name, typeDef.fmt, typeDef)

        if len != 1:
            size = struct.calcsize(typeDef)
            if typeDef not in "xspP":
                typeDef = 's'
            typeDef = str(len * size) + typeDef

        fmtChar = typeDef[-1:]

        if fmtChar == 'x':
            typeObj = Structure.skipType
        else:
            typeObj = _TypeWriter(Structure.defaults[fmtChar], reader, writer)

        return (name, typeDef, typeObj)

    def yieldArgs(self, keyArgs):
        """ Take (nested) dict(s) of args to set, and return flat list of args. """
        for (name, typeObj) in self._types.items():
            for arg in typeObj.yieldArgs(keyArgs.get(name, None)):
                yield arg

    def write(self, keyArgs):
        """ Write specified key arguments into data structure. """
        # bytearray doesn't work with fcntl
        args = array.array('B', (0,) * self.size)
        self._struct.pack_into(args, 0, *list(self.yieldArgs(keyArgs)))
        return args

    def popValue(self, argList):
        """ Take a flat arglist, and pop relevent values and return as a value or tuple. """
        # return self._Tuple(*[name for (name, typeObj) in self._types.items()])
        return self._Tuple(*[typeObj.popValue(argList) for (name, typeObj) in self._types.items()])

    def read(self, data, offset=0):
        """ Read data structure and return (nested) named tuple(s). """
        if isinstance(data, Buffer):
            return data.read(self)

        try:
            args = list(self._struct.unpack_from(data, offset))
        except TypeError as error:
            # Working around struct.unpack_from issue #10212
            logger.debug("error: %s", error)
            args = list(self._struct.unpack_from(str(bytearray(data)), offset))
        args.reverse()
        return self.popValue(args)


class Buffer:

    """ Contains bytes and an offset. """

    def __init__(self, buf, offset=0, newLength=None):
        """ Initialize. """
        self.buf = buf
        self.offset = offset
        self._len = (newLength + offset) if newLength else len(buf)

    def read(self, structure):
        """ Read and advance. """
        start = self.offset
        self.skip(structure.size)
        return structure.read(self.buf, start)

    def skip(self, length):
        """ Advance. """
        self.offset += length

    def readView(self, newLength=None):
        """ Return a view of the next newLength bytes, and skip it. """
        if newLength is None:
            newLength = self.len
        result = self.peekView(newLength)
        self.skip(newLength)
        return result

    def peekView(self, newLength):
        """ Return a view of the next newLength bytes. """
        # Note: In Python 2.7, memoryviews can't be written to
        # by the struct module. (BUG)
        return memoryview(self.buf)[self.offset:self.offset + newLength]

    def readBuffer(self, newLength):
        """ Read next chunk as another buffer. """
        result = Buffer(self.buf, self.offset, newLength)
        self.skip(newLength)
        return result

    @property
    def len(self):
        """ Count of remaining bytes. """
        return self._len - self.offset

    def __len__(self):
        """ Count of remaining bytes. """
        return self._len - self.offset


class Control:

    """ Callable linux io control (ioctl). """

    def __init__(self, direction, op, structure):
        """ Initialize. """
        self.structure = structure
        size = structure.size if structure else 0
        self.ioc = self._iocNumber(direction, self.magic, op, size)

    def __call__(self, device, **args):
        """ Execute the call. """
        if device.fd is None:
            raise Exception("Device hasn't been successfully opened.  Use 'with' statement.")

        try:
            if self.structure is not None:
                args = self.structure.write(args)
                # log.write(args)
                ret = fcntl.ioctl(device.fd, self.ioc, args, True)
                # log.write(args)
                assert ret == 0, ret
                return self.structure.read(args)
            else:
                ret = fcntl.ioctl(device.fd, self.ioc)
                assert ret == 0, ret
        except IOError as error:
            error.filename = device.path
            raise

    @staticmethod
    def _iocNumber(dir, type, nr, size):
        return dir << DIRSHIFT | \
            type << TYPESHIFT | \
            nr << NRSHIFT | \
            size << SIZESHIFT

    @classmethod
    def _IOC(cls, dir, op, structure=None):
        """ Encode an ioctl id. """
        control = cls(dir, op, structure)

        def do(dev, **args):
            return control(dev, **args)
        return do

    @classmethod
    def IO(cls, op):
        """ Returns an ioctl Device method with no arguments. """
        return cls._IOC(NONE, op)

    @classmethod
    def IOW(cls, op, structure):
        """ Returns an ioctl Device method with WRITE arguments. """
        return cls._IOC(WRITE, op, structure)

    @classmethod
    def IOWR(cls, op, structure):
        """ Returns an ioctl Device method with READ and WRITE arguments. """
        return cls._IOC(READ | WRITE, op, structure)

    @classmethod
    def IOR(cls, op, structure):
        """ Returns an ioctl Device method with READ arguments. """
        return cls._IOC(READ, op, structure)


class Device(object):

    """ Context manager for a linux file descriptor for a file or device special file.

    Opening and closing is handled by the Python "with" statement.

    """

    def __init__(self, path, flags=os.O_RDONLY):
        """ Initialize. """
        self.path = path
        self.fd = None
        self.flags = flags

    def __enter__(self):
        """ Open. """
        self.fd = os.open(self.path, self.flags)
        return self

    def __exit__(self, exceptionType, exception, trace):
        """ Close. """
        os.close(self.fd)
        self.fd = None
