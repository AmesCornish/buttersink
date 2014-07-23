""" Utilities for working with fnctl.ioctl. """

# See <linux/ioctl.h> for C source.

import array
import fcntl
import os
import struct

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


# structure values are ((fmt, name), ...) tuples
# fmt can be a string or a structure itself


def format(structure):
    """ Return structure module format string for structure tuple. """
    if isinstance(structure, (basestring, str, unicode)):
        return structure

    return "".join(format(fmt) for (fmt, name) in structure)


def sizeof(structure):
    """ Return size in bytes of (packed) structure tuple. """
    return struct.calcsize(format(structure))


# structure format strings for C type definitions

class t:

    """ Convenient types for translating linux headers to Python struct. """

    u64 = 'Q'
    u32 = 'L'

    max_u64 = (1 << 64) - 1

    @staticmethod
    def char(len):
        """ char fieldName[len]. """
        return str(len) + 's'

    @staticmethod
    def u8(len):
        """ __u8 fieldName[len]. """
        return t.char(len)

    @staticmethod
    def pad_64(len):
        """ Number of reserved u64. """
        return t.pad_8(8*len)

    @staticmethod
    def pad_8(len):
        """ Number of reserved u8. """
        return str(len) + 'x'

    
class Control:

    """ Callable linux io control (ioctl). """

    def __init__(self, direction, op, structure):
        """ Initialize. """
        self.structure = structure
        self.fmt = struct.Struct(format(structure))
        self.ioc = self._ioc(direction, self.magic, op, self.fmt.size)

    def __call__(self, device, **args):
        """ Execute the call. """
        args = self._packArgs(**args)
        ret = fcntl.ioctl(device.fd, self.ioc, args, True)
        assert ret == 0, ret
        results = self._unpackArgs(args)
        return results

    @staticmethod
    def _ioc(dir, type, nr, size):
        return dir << DIRSHIFT | \
            type << TYPESHIFT | \
            nr << NRSHIFT | \
            size << SIZESHIFT

    @classmethod
    def IOC(cls, dir, op, structure=None):
        """ Encode an ioctl id. """
        control = cls(dir, op, structure)

        def do(dev, **args):
            return control(dev, **args)
        return do

    @classmethod
    def IO(cls, op):
        """ ioctl id with no arguments. """
        return cls.IOC(NONE, op)

    @classmethod
    def IOW(cls, op, structure):
        """ ioctl id with WRITE arguments. """
        return cls.IOC(WRITE, op, structure)

    @classmethod
    def IOWR(cls, op, structure):
        """ ioctl id with READ and WRITE arguments. """
        return cls.IOC(READ | WRITE, op, structure)

    @classmethod
    def IOR(cls, op, structure):
        """ ioctl id with READ arguments. """
        return cls.IOC(READ, op, structure)

    @staticmethod
    def _listArgs(structure, keyArgs):
        if isinstance(structure, (str, unicode, basestring)):
            if keyArgs is None:
                cType = structure[-1:]
                if cType in "sp":
                    yield ""
                elif cType in "c":
                    yield "\0"
                elif cType in "?":
                    yield False
                elif cType in "x":
                    pass
                else:
                    yield 0
            else:
                yield keyArgs
            return

        for (fmt, name) in structure:
            for value in Control._listArgs(fmt, keyArgs.get(name, None)):
                yield value

    def _packArgs(self, **keyArgs):
        args = array.array('B', (0,)*self.fmt.size)
        # args = bytearray(struct.calcsize(fmt))  # bytearray doesn't work with fcntl
        self.fmt.pack_into(args, 0, *list(Control._listArgs(self.structure, keyArgs)))
        return args

    @staticmethod
    def _dictArgs(structure, argList):
        if isinstance(structure, (str, unicode, basestring)):
            cType = structure[-1:]
            if cType in "x":
                return None
            return argList.pop()

        return {name: Control._dictArgs(fmt, argList) for (fmt, name) in structure}

    def _unpackArgs(self, args):
        argList = list(self.fmt.unpack_from(args, 0))
        argList.reverse()
        return Control._dictArgs(self.structure, argList)


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
