""" Pythonic wrapper around kernel ioctl-level access to btrfs. """

# See <linux/btrfs.h> for C source.
# See btrfs-progs/ctree.h for some constants.
# Headers taken from 3.13.0-32

# This is NOT a complete implementation,
# just a few useful routines for my current project.

from ioctl import Structure, t
import collections
import ioctl
import logging
import pprint
logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')


def pretty(obj):
    """ Return pretty representation of obj. """
    # if True:
    #     return pprint.pformat(dict(obj.__dict__))
    return pprint.pformat(obj)
    # try:
    #     return pprint.pformat(dict(obj))
    # except TypeError:
    #     try:
    #         return pprint.pformat(obj.__dict__)
    #     except KeyError:
    #         logger.exception("Funny error.")


def bytes2uuid(b):
    """ Return standard human-friendly UUID. """
    if b.strip(chr(0)) == '':
        return None

    s = b.encode('hex')
    return "%s-%s-%s-%s-%s" % (s[0:8], s[8:12], s[12:16], s[16:20], s[20:])


def uuid2bytes(u):
    """ Return compact bytes for UUID. """
    if u is None:
        return ''

    return "".join(u.split('-')).decode('hex')

BTRFS_DEVICE_PATH_NAME_MAX = 1024
BTRFS_SUBVOL_CREATE_ASYNC = (1 << 0)
BTRFS_SUBVOL_RDONLY = (1 << 1)
BTRFS_SUBVOL_QGROUP_INHERIT = (1 << 2)
BTRFS_FSID_SIZE = 16
BTRFS_UUID_SIZE = 16

BTRFS_INO_LOOKUP_PATH_MAX = 4080
btrfs_ioctl_ino_lookup_args = Structure(
    (t.u64, 'treeid'),
    (t.u64, 'objectid'),
    (t.char, 'name', BTRFS_INO_LOOKUP_PATH_MAX, t.readString, t.writeString),
    packed=True
)

btrfs_ioctl_search_key = Structure(
    (t.u64, 'tree_id'),
    (t.u64, 'min_objectid'),
    (t.u64, 'max_objectid'),
    (t.u64, 'min_offset'),
    (t.u64, 'max_offset'),
    (t.u64, 'min_transid'),
    (t.u64, 'max_transid'),
    (t.u32, 'min_type'),
    (t.u32, 'max_type'),
    (t.u32, 'nr_items'),
    (t.u32, 'unused'),
    (t.u64, 'unused1'),
    (t.u64, 'unused2'),
    (t.u64, 'unused3'),
    (t.u64, 'unused4'),
    packed=True
)

btrfs_ioctl_search_header = Structure(
    (t.u64, 'transid'),
    (t.u64, 'objectid'),
    (t.u64, 'offset'),
    (t.u32, 'type'),
    (t.u32, 'len'),
    packed=True
)

btrfs_ioctl_dev_info_args = Structure(
    (t.u64, 'devid'),                # /* in/out */
    (t.u8, 'uuid', BTRFS_UUID_SIZE, bytes2uuid, uuid2bytes),     # /* in/out */
    (t.u64, 'bytes_used'),           # /* out */
    (t.u64, 'total_bytes'),          # /* out */
    (t.u64, 'unused', 379, t.readBuffer),          # /* pad to 4k */
    (t.u8, 'path', BTRFS_DEVICE_PATH_NAME_MAX, t.readString, t.writeString),  # /* out */
    packed=True
)

btrfs_ioctl_fs_info_args = Structure(
    (t.u64, 'max_id'),               # /* out */
    (t.u64, 'num_devices'),          # /* out */
    (t.u8, 'fsid', BTRFS_FSID_SIZE, bytes2uuid, uuid2bytes),     # /* out */
    (t.u64, 'reserved', 124, t.readBuffer),            # /* pad to 1k */
    packed=True
)


BTRFS_SEARCH_ARGS_BUFSIZE = (4096 - btrfs_ioctl_search_key.size)

btrfs_ioctl_search_args = Structure(
    (btrfs_ioctl_search_key, 'key'),
    (t.char, 'buf', BTRFS_SEARCH_ARGS_BUFSIZE),
    packed=True
)

# From btrfs-progs ctree.h

btrfs_disk_key = Structure(
    (t.le64, 'objectid'),
    (t.u8, 'type'),
    (t.le64, 'offset'),
    packed=True
)

btrfs_timespec = Structure(
    (t.le64, 'sec'),
    (t.le32, 'nsec'),
    packed=True
)

btrfs_inode_item = Structure(
    (t.le64, 'generation'),
    (t.le64, 'transid'),
    (t.le64, 'size'),
    (t.le64, 'nbytes'),
    (t.le64, 'block_group'),
    (t.le32, 'nlink'),
    (t.le32, 'uid'),
    (t.le32, 'gid'),
    (t.le32, 'mode'),
    (t.le64, 'rdev'),
    (t.le64, 'flags'),
    (t.le64, 'sequence'),
    (t.le64, 'reserved', 4, t.readBuffer),
    (btrfs_timespec, 'atime'),
    (btrfs_timespec, 'ctime'),
    (btrfs_timespec, 'mtime'),
    (btrfs_timespec, 'otime'),
    packed=True
)

btrfs_root_item_v0 = Structure(
    (btrfs_inode_item, 'inode'),
    (t.le64, 'generation'),
    (t.le64, 'root_dirid'),
    (t.le64, 'bytenr'),
    (t.le64, 'byte_limit'),
    (t.le64, 'bytes_used'),
    (t.le64, 'last_snapshot'),
    (t.le64, 'flags'),
    (t.le32, 'refs'),
    (btrfs_disk_key, 'drop_progress'),
    (t.u8, 'drop_level'),
    (t.u8, 'level'),
    packed=True
)

btrfs_root_item = Structure(
    (btrfs_inode_item, 'inode'),
    (t.le64, 'generation'),
    (t.le64, 'root_dirid'),
    (t.le64, 'bytenr'),
    (t.le64, 'byte_limit'),
    (t.le64, 'bytes_used'),
    (t.le64, 'last_snapshot'),
    (t.le64, 'flags'),
    (t.le32, 'refs'),
    (btrfs_disk_key, 'drop_progress'),
    (t.u8, 'drop_level'),
    (t.u8, 'level'),
    (t.le64, 'generation_v2'),
    (t.u8, 'uuid', BTRFS_UUID_SIZE, bytes2uuid, uuid2bytes),
    (t.u8, 'parent_uuid', BTRFS_UUID_SIZE, bytes2uuid, uuid2bytes),
    (t.u8, 'received_uuid', BTRFS_UUID_SIZE, bytes2uuid, uuid2bytes),
    (t.le64, 'ctransid'),
    (t.le64, 'otransid'),
    (t.le64, 'stransid'),
    (t.le64, 'rtransid'),
    (btrfs_timespec, 'ctime'),
    (btrfs_timespec, 'otime'),
    (btrfs_timespec, 'stime'),
    (btrfs_timespec, 'rtime'),
    (t.le64, 'reserved', 8, t.readBuffer),
    packed=True
)

btrfs_root_ref = Structure(
    (t.le64, 'dirid'),
    (t.le64, 'sequence'),
    (t.le16, 'name_len'),
    packed=True
)

BTRFS_IOCTL_MAGIC = 0x94

BTRFS_ROOT_TREE_OBJECTID = 1

BTRFS_ROOT_ITEM_KEY = 132
BTRFS_ROOT_BACKREF_KEY = 144
BTRFS_ROOT_REF_KEY = 156
BTRFS_FIRST_FREE_OBJECTID = 256
BTRFS_LAST_FREE_OBJECTID = (1 << 64) - 256
BTRFS_FIRST_CHUNK_TREE_OBJECTID = 256


class Volume(object):

    """ Represents a subvolume. """

    volumes = {}

    def __init__(self, rootid, generation, info):
        """ Initialize. """
        # logger.debug("Volume %d: %s", id, pretty(info))
        self.id = rootid  # id in BTRFS_ROOT_TREE_OBJECTID, also FS treeid for this volume
        self.gen = generation
        self.size = info.bytes_used
        self.level = info.level
        self.uuid = info.uuid
        self.parent_uuid = info.parent_uuid
        self.received_uuid = info.received_uuid

        self.info = info

        self.links = {}

        assert rootid not in Volume.volumes, rootid
        Volume.volumes[rootid] = self

        logger.debug("%s", self)

    def _addLink(self, dirTree, dirID, dirSeq, dirPath, name):
        """ Add tree reference and name. (Hardlink). """
        logger.debug("Link  %d-%d-%d '%s%s'", dirTree, dirID, dirSeq, dirPath, name)
        assert (dirTree, dirID, dirSeq) not in self.links, (dirTree, dirID, dirSeq)
        self.links[(dirTree, dirID, dirSeq)] = (dirPath, name)
        assert len(self.links) == 1, self.links  # Cannot have multiple hardlinks to a directory
        logger.debug("%s", self)

    @property
    def fullPath(self):
        """ Return full butter path from butter root. """
        for ((dirTree, dirID, dirSeq), (dirPath, name)) in self.links.items():
            return Volume.volumes[dirTree].fullPath + "/" + dirPath + name
        return ""

    def __str__(self):
        """ String representation. """
        # return pretty(self.__dict__)
        return "%4d '%s' (level:%d gen:%d size:%d)\n\t%s (parent:%s received:%s)" % (
            self.id,
            # ", ".join([dirPath + name for (dirPath, name) in self.links.values()]),
            self.fullPath,
            self.level,
            self.gen,
            self.size,
            self.uuid,
            self.parent_uuid,
            self.received_uuid,
        )


class Control(ioctl.Control):

    """ A btrfs IOCTL. """

    magic = BTRFS_IOCTL_MAGIC


class Mount(ioctl.Device):

    """ Mounted file system descriptor for ioctl actions. """

    def __init__(self, path):
        """ Initialize. """
        super(Mount, self).__init__(path)
        self.volumes = None

    @property
    def subvolumes(self):
        """ Subvolumes contained in this mount. """
        self._getTree()
        volumes = Volume.volumes.values()
        volumes.sort(key=(lambda v: v.fullPath))
        return volumes

    def _getTree(self):
        Key = collections.namedtuple('Key', ('objectid', 'type', 'offset'))

        key = Key(
            objectid=0,
            type=0,
            # objectid=BTRFS_FIRST_FREE_OBJECTID,
            # type=BTRFS_ROOT_ITEM_KEY,
            offset=0,
        )

        endKey = Key(
            objectid=BTRFS_LAST_FREE_OBJECTID,
            type=BTRFS_ROOT_BACKREF_KEY,
            offset=t.max_u64,
        )

        Key.next = (lambda key: Key(key.objectid, key.type, key.offset + 1))

        self.IOC_SYNC()

        while True:
            # logger.debug("Min obj %d, offset %d", objectID, offset)

            # Returned objects seem to be monotonically increasing in (objectid, type, offset)
            # min and max values are *not* filters.
            result = self.TREE_SEARCH(
                key=dict(
                    tree_id=BTRFS_ROOT_TREE_OBJECTID,
                    min_type=key.type,
                    max_type=endKey.type,
                    min_objectid=key.objectid,
                    max_objectid=endKey.objectid,
                    min_offset=key.offset,
                    max_offset=endKey.offset,
                    min_transid=0,
                    max_transid=t.max_u64,
                    nr_items=4096,
                ),
            )
            # logger.debug("Search key result: \n%s", pretty(result.key))

            buf = ioctl.Buffer(result.buf)
            results = result.key.nr_items

            logger.debug("Reading %d nodes from %d bytes", results, buf.len)
            if results == 0:
                break

            for i in xrange(results):
                assert buf.len >= btrfs_ioctl_search_header.size, buf.len

                data = buf.read(btrfs_ioctl_search_header)

                # logger.debug("Object %d: %s", i, pretty(data))

                assert buf.len >= data.len, (buf.len, data.len)

                # "key" values
                key = Key(data.objectid, data.type, data.offset)

                if data.type == BTRFS_ROOT_BACKREF_KEY:
                    info = buf.read(btrfs_root_ref)

                    nS = Structure((t.char, 'name', data.len - btrfs_root_ref.size))
                    name = buf.read(nS).name

                    directory = self.INO_LOOKUP(treeid=key.offset, objectid=info.dirid,)

                    logger.debug("%s: %s %s", name, pretty(info), pretty(directory))

                    Volume.volumes[key.objectid]._addLink(
                        key.offset,
                        info.dirid,
                        info.sequence,
                        directory.name,
                        name,
                    )
                elif data.type == BTRFS_ROOT_ITEM_KEY:
                    if data.len == btrfs_root_item.size:
                        info = buf.read(btrfs_root_item)
                    elif data.len == btrfs_root_item_v0.size:
                        info = buf.read(btrfs_root_item_v0)
                    else:
                        assert False, data.len

                    Volume(
                        key.objectid,
                        key.offset,
                        info,
                    )
                else:
                    buf.skip(data.len)
                    continue

            key = key.next()

    def _getDevInfo(self):
        return self.DEV_INFO(devid=1, uuid="")

    def _getFSInfo(self):
        return self.FS_INFO()

    IOC_SYNC = Control.IO(8)
    TREE_SEARCH = Control.IOWR(17, btrfs_ioctl_search_args)
    INO_LOOKUP = Control.IOWR(18, btrfs_ioctl_ino_lookup_args)
    DEV_INFO = Control.IOWR(30, btrfs_ioctl_dev_info_args)
    FS_INFO = Control.IOR(31, btrfs_ioctl_fs_info_args)

# #define BTRFS_IOC_DEFAULT_SUBVOL _IOW(BTRFS_IOCTL_MAGIC, 19, __u64)
# #define BTRFS_IOC_INO_PATHS _IOWR(BTRFS_IOCTL_MAGIC, 35, \
#                     struct btrfs_ioctl_ino_path_args)
# #define BTRFS_IOC_LOGICAL_INO _IOWR(BTRFS_IOCTL_MAGIC, 36, \
#                     struct btrfs_ioctl_ino_path_args)
# #define BTRFS_IOC_SET_RECEIVED_SUBVOL _IOWR(BTRFS_IOCTL_MAGIC, 37, \
#                 struct btrfs_ioctl_received_subvol_args)
# #define BTRFS_IOC_SEND _IOW(BTRFS_IOCTL_MAGIC, 38, struct btrfs_ioctl_send_args)
# #define BTRFS_IOC_QUOTA_RESCAN _IOW(BTRFS_IOCTL_MAGIC, 44, \
#                    struct btrfs_ioctl_quota_rescan_args)
# #define BTRFS_IOC_QUOTA_RESCAN_STATUS _IOR(BTRFS_IOCTL_MAGIC, 45, \
#                    struct btrfs_ioctl_quota_rescan_args)
# #define BTRFS_IOC_QUOTA_RESCAN_WAIT _IO(BTRFS_IOCTL_MAGIC, 46)
# #define BTRFS_IOC_QUOTA_CTL _IOWR(BTRFS_IOCTL_MAGIC, 40, \
#                    struct btrfs_ioctl_quota_ctl_args)
