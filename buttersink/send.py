""" Utilities for sending and receiving btrfs snapshots. """

# Docs: See btrfs-progs/send-utils.c, btrfs-progs/send.h

from ioctl import Structure, t
import btrfs
import ioctl

# import binascii  # This provides "zip" crc
# import crc32c  # This provides *slow* btrfs crc32c
import crcmod.predefined  # This provides fast compiled extension
crc32c = crcmod.predefined.mkPredefinedCrcFun("crc-32c")

import logging
import struct

logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')

BTRFS_SEND_STREAM_MAGIC = "btrfs-stream\0"
BTRFS_SEND_STREAM_VERSION = 1

btrfs_stream_header = Structure(
    (t.char, 'magic', len(BTRFS_SEND_STREAM_MAGIC)),
    (t.le32, 'version'),
    packed=True
)

btrfs_cmd_header = Structure(
    # /* len excluding the header */
    (t.le32, 'len'),
    (t.le16, 'cmd'),
    # /* crc including the header with zero crc field */
    (t.le32, 'crc'),
    packed=True
)

btrfs_tlv_header = Structure(
    (t.le16, 'tlv_type'),
    # /* len excluding the header */
    (t.le16, 'tlv_len'),
    packed=True
)

# /* commands */
(
    BTRFS_SEND_C_UNSPEC,

    BTRFS_SEND_C_SUBVOL,
    BTRFS_SEND_C_SNAPSHOT,

    BTRFS_SEND_C_MKFILE,
    BTRFS_SEND_C_MKDIR,
    BTRFS_SEND_C_MKNOD,
    BTRFS_SEND_C_MKFIFO,
    BTRFS_SEND_C_MKSOCK,
    BTRFS_SEND_C_SYMLINK,

    BTRFS_SEND_C_RENAME,
    BTRFS_SEND_C_LINK,
    BTRFS_SEND_C_UNLINK,
    BTRFS_SEND_C_RMDIR,

    BTRFS_SEND_C_SET_XATTR,
    BTRFS_SEND_C_REMOVE_XATTR,

    BTRFS_SEND_C_WRITE,
    BTRFS_SEND_C_CLONE,

    BTRFS_SEND_C_TRUNCATE,
    BTRFS_SEND_C_CHMOD,
    BTRFS_SEND_C_CHOWN,
    BTRFS_SEND_C_UTIMES,

    BTRFS_SEND_C_END,
    BTRFS_SEND_C_UPDATE_EXTENT,
    __BTRFS_SEND_C_MAX,
) = range(24)
BTRFS_SEND_C_MAX = (__BTRFS_SEND_C_MAX - 1)

# /* attributes in send stream */
(
    BTRFS_SEND_A_UNSPEC,

    BTRFS_SEND_A_UUID,
    BTRFS_SEND_A_CTRANSID,

    BTRFS_SEND_A_INO,
    BTRFS_SEND_A_SIZE,
    BTRFS_SEND_A_MODE,
    BTRFS_SEND_A_UID,
    BTRFS_SEND_A_GID,
    BTRFS_SEND_A_RDEV,
    BTRFS_SEND_A_CTIME,
    BTRFS_SEND_A_MTIME,
    BTRFS_SEND_A_ATIME,
    BTRFS_SEND_A_OTIME,

    BTRFS_SEND_A_XATTR_NAME,
    BTRFS_SEND_A_XATTR_DATA,

    BTRFS_SEND_A_PATH,
    BTRFS_SEND_A_PATH_TO,
    BTRFS_SEND_A_PATH_LINK,

    BTRFS_SEND_A_FILE_OFFSET,
    BTRFS_SEND_A_DATA,

    BTRFS_SEND_A_CLONE_UUID,
    BTRFS_SEND_A_CLONE_CTRANSID,
    BTRFS_SEND_A_CLONE_PATH,
    BTRFS_SEND_A_CLONE_OFFSET,
    BTRFS_SEND_A_CLONE_LEN,

    __BTRFS_SEND_A_MAX,
) = range(26)
BTRFS_SEND_A_MAX = (__BTRFS_SEND_A_MAX - 1)


class ParseException(Exception):

    """ Dedicated Exception class. """

    pass


def TLV_GET(attrs, attrNum, format):
    """ Get a tag-length-value encoded attribute. """
    attrView = attrs[attrNum]
    if format == 's':
        format = str(attrView.len) + format
    try:
        (result,) = struct.unpack_from(format, attrView.buf, attrView.offset)
    except TypeError:
        # Working around struct.unpack_from issue #10212
        (result,) = struct.unpack_from(format, str(bytearray(attrView.buf)), attrView.offset)
    return result


def TLV_PUT(attrs, attrNum, format, value):
    """ Put a tag-length-value encoded attribute. """
    attrView = attrs[attrNum]
    if format == 's':
        format = str(attrView.len) + format
    struct.pack_into(format, attrView.buf, attrView.offset, value)


def TLV_GET_BYTES(attrs, attrNum):
    """ Get a tag-length-value encoded attribute as bytes. """
    return TLV_GET(attrs, attrNum, 's')


def TLV_PUT_BYTES(attrs, attrNum, value):
    """ Put a tag-length-value encoded attribute as bytes. """
    TLV_PUT(attrs, attrNum, 's', value)


def TLV_GET_STRING(attrs, attrNum):
    """ Get a tag-length-value encoded attribute as a string. """
    return t.readString(TLV_GET_BYTES(attrs, attrNum))


def TLV_GET_UUID(attrs, attrNum):
    """ Get a tag-length-value encoded attribute as a UUID. """
    return btrfs.bytes2uuid(TLV_GET_BYTES(attrs, attrNum))


def TLV_GET_U64(attrs, attrNum):
    """ Get a tag-length-value encoded attribute as a U64. """
    return TLV_GET(attrs, attrNum, t.u64)


def replaceIDs(data, receivedUUID, receivedGen, parentUUID, parentGen):
    """ Parse and replace UUID and transid info in data stream. """
    if len(data) < 20:
        return data

    logger.debug(
        "Setting received %s/%d and parent %s/%d",
        receivedUUID, receivedGen or 0, parentUUID, parentGen or 0.
        )
    data = bytearray(data)  # Make data writable

    buf = ioctl.Buffer(data)
    header = buf.read(btrfs_stream_header)

    if header.magic != BTRFS_SEND_STREAM_MAGIC:
        raise ParseException("Didn't find '%s'" % (BTRFS_SEND_STREAM_MAGIC))

    logger.debug("Version: %d", header.version)

    if header.version > BTRFS_SEND_STREAM_VERSION:
        logger.warn("Unknown stream version: %d", header.version)

    cmdHeaderView = buf.peekView(btrfs_cmd_header.size)
    cmdHeader = buf.read(btrfs_cmd_header)

    logger.debug("Command: %d", cmdHeader.cmd)

    # Read the attributes

    attrs = {}
    attrDataView = buf.peekView(cmdHeader.len)
    attrData = buf.readBuffer(cmdHeader.len)

    while attrData.len > 0:
        attrHeader = attrData.read(btrfs_tlv_header)
        attrs[attrHeader.tlv_type] = attrData.readBuffer(attrHeader.tlv_len)

    def calcCRC():
        header = vars(cmdHeader)
        header['crc'] = 0

        # This works, but is slow
        # crc = crc32c.CRC_INIT ^ crc32c._MASK
        # crc = crc32c.crc_update(crc, btrfs_cmd_header.write(header))
        # crc = crc32c.crc_update(crc, attrDataView)
        # crc = crc32c.crc_finalize(crc)
        # crc = crc ^ crc32c._MASK

        # This works, and can be fast, when it used compiled extension
        crc = 0 ^ 0xffffffff
        crc = crc32c(btrfs_cmd_header.write(header), crc)
        crc = crc32c(attrDataView.tobytes(), crc)
        crc &= 0xffffffff
        crc = crc ^ 0xffffffff

        # This does *not* work
        # crc = 0 ^ 0xffffffff
        # crc = binascii.crc32(btrfs_cmd_header.write(header), crc)
        # crc = binascii.crc32(attrDataView, crc)
        # crc &= 0xffffffff
        # crc ^= 0xffffffff

        return crc

    crc = calcCRC()
    if cmdHeader.crc != crc:
        logger.warn(
            "Stored crc (%d) doesn't match calculated crc (%d)",
            cmdHeader.crc, crc,
            )

    # Dispatch based on cmd and attributes

    s = attrs

    def correct(attr, format, name, old, new, encode=None):
        if new is not None and new != old:
            logger.debug("Correcting %s from %s to %s", name, str(old), str(new))
            if encode is not None:
                new = encode(new)
            TLV_PUT(attrs, attr, format, new)

    def correctCRC():
        crc = calcCRC()
        if cmdHeader.crc != crc:
            logger.debug("Correcting CRC from %d to %d", cmdHeader.crc, crc)
            header = vars(cmdHeader)
            header['crc'] = crc
            cmdHeaderView[:] = btrfs_cmd_header.write(header).tostring()

    if cmdHeader.cmd == BTRFS_SEND_C_SUBVOL:
        path = TLV_GET_STRING(s, BTRFS_SEND_A_PATH, )
        uuid = TLV_GET_UUID(s, BTRFS_SEND_A_UUID, )
        ctransid = TLV_GET_U64(s, BTRFS_SEND_A_CTRANSID, )

        logger.debug('Subvol: %s/%d %s', uuid, ctransid, path)

        correct(
            BTRFS_SEND_A_UUID,
            's',
            'received UUID',
            uuid,
            receivedUUID,
            btrfs.uuid2bytes
        )
        correct(
            BTRFS_SEND_A_CTRANSID,
            t.u64,
            'received gen',
            ctransid,
            receivedGen
        )

    elif cmdHeader.cmd == BTRFS_SEND_C_SNAPSHOT:
        path = TLV_GET_STRING(s, BTRFS_SEND_A_PATH, )
        uuid = TLV_GET_UUID(s, BTRFS_SEND_A_UUID, )
        ctransid = TLV_GET_U64(s, BTRFS_SEND_A_CTRANSID, )
        clone_uuid = TLV_GET_UUID(s, BTRFS_SEND_A_CLONE_UUID, )
        clone_ctransid = TLV_GET_U64(s, BTRFS_SEND_A_CLONE_CTRANSID, )

        logger.debug(
            'Snapshot: %s/%d -> %s/%d %s',
            clone_uuid, clone_ctransid, uuid, ctransid, path
        )

        correct(
            BTRFS_SEND_A_UUID,
            's',
            'received UUID',
            uuid,
            receivedUUID,
            btrfs.uuid2bytes
        )
        correct(
            BTRFS_SEND_A_CTRANSID,
            t.u64,
            'received gen',
            ctransid,
            receivedGen
        )

        correct(
            BTRFS_SEND_A_CLONE_UUID,
            's',
            'parent UUID',
            clone_uuid,
            parentUUID,
            btrfs.uuid2bytes
        )
        correct(
            BTRFS_SEND_A_CLONE_CTRANSID,
            t.u64,
            'parent gen',
            clone_ctransid,
            parentGen
        )
    else:
        logger.warn("Didn't find volume UUID command")

    correctCRC()

    return data
