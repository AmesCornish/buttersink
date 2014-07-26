""" Pythonic wrapper around kernel ioctl-level access to btrfs. """

# See <linux/btrfs.h> for C source.
# See btrfs-progs/ctree.h for some constants.
# Headers taken from 3.13.0-32

# This is NOT a complete implementation,
# just a few useful routines for my current project.

from ioctl import Structure, t
import ioctl
import logging
import pprint
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


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
    (t.char, 'name', BTRFS_INO_LOOKUP_PATH_MAX),
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
    (t.u8, 'uuid', BTRFS_UUID_SIZE),     # /* in/out */
    (t.u64, 'bytes_used'),           # /* out */
    (t.u64, 'total_bytes'),          # /* out */
    (t.u64, 'unused', 379),          # /* pad to 4k */
    (t.u8, 'path', BTRFS_DEVICE_PATH_NAME_MAX),  # /* out */
    packed=True
)

btrfs_ioctl_fs_info_args = Structure(
    (t.u64, 'max_id'),               # /* out */
    (t.u64, 'num_devices'),          # /* out */
    (t.u8, 'fsid', BTRFS_FSID_SIZE),     # /* out */
    (t.u64, 'reserved', 124),            # /* pad to 1k */
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
    (t.le64, 'reserved', 4),
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
    (t.u8, 'uuid', BTRFS_UUID_SIZE),
    (t.u8, 'parent_uuid', BTRFS_UUID_SIZE),
    (t.u8, 'received_uuid', BTRFS_UUID_SIZE),
    (t.le64, 'ctransid'),
    (t.le64, 'otransid'),
    (t.le64, 'stransid'),
    (t.le64, 'rtransid'),
    (btrfs_timespec, 'ctime'),
    (btrfs_timespec, 'otime'),
    (btrfs_timespec, 'stime'),
    (btrfs_timespec, 'rtime'),
    (t.le64, 'reserved', 8),
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


def bytes2uuid(b):
    """ Return standard human-friendly UUID. """
    s = b.encode('hex')
    return "%s-%s-%s-%s-%s" % (s[0:8], s[8:12], s[12:16], s[16:20], s[20:])


class Volume(object):

    """ Represents a subvolume. """

    volumes = {}

    def __init__(self, inodeItem, inodeRef, info):
        """ Initialize. """
        # logger.debug("Volume %d: %s", id, pretty(info))
        self.inode = inodeItem
        self.ref = inodeRef
        self.info = info
        self.trees = {}
        self.uuid = bytes2uuid(info.uuid)
        self.parent_uuid = bytes2uuid(info.parent_uuid)
        self.received_uuid = bytes2uuid(info.received_uuid)
        assert inodeItem not in Volume.volumes, inodeItem
        Volume.volumes[inodeItem] = self
        logger.debug("%s", self)

    def _addLink(self, parentDir, info, name):
        """ Add tree reference and name. (Hardlink). """
        logger.debug("Link  %d '%s' %s", parentDir, name, pretty(info))
        assert (parentDir, info.sequence) not in self.trees, parentDir
        self.trees[(parentDir, info.sequence)] = (name, info)
        assert len(self.trees) == 1, self.trees  # Cannot have multiple hardlinks to a directory
        logger.debug("%s", self)

    def __str__(self):
        """ String representation. """
        return "'%s' %5d-%d %s\n          (parent %s received %s) " % (
            ", ".join([name for (name, info) in self.trees.values()]),
            self.inode,
            self.ref,
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
        return Volume.volumes

    def _getTree(self):
        objectID = BTRFS_FIRST_FREE_OBJECTID
        typeNum = BTRFS_ROOT_ITEM_KEY
        offset = 0

        while True:
            # logger.debug("Min obj %d, offset %d", objectID, offset)

            # Returned objects seem to be monotonically increasing in (objectid, type, offset)
            # min_type and max_type don't really work.
            result = self.TREE_SEARCH(
                key=dict(
                    tree_id=BTRFS_ROOT_TREE_OBJECTID,
                    min_type=typeNum,  # This has no effect
                    max_type=BTRFS_ROOT_BACKREF_KEY,  # This has no effect
                    min_objectid=objectID,
                    max_objectid=BTRFS_LAST_FREE_OBJECTID,
                    min_offset=offset,  # This has no effect
                    max_offset=t.max_u64,
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

            stale = True

            for i in xrange(results):
                assert buf.len >= btrfs_ioctl_search_header.size, buf.len

                data = buf.read(btrfs_ioctl_search_header)

                logger.debug("Object %d: %s", i, pretty(data))

                assert buf.len >= data.len, (buf.len, data.len)

                if stale:
                    if (
                        objectID >= data.objectid and
                        typeNum >= data.type and
                        offset >= data.offset
                    ):
                        logger.warn("Skipping stale object")
                        buf.skip(data.len)
                        continue

                stale = False

                # "key" values
                objectID = data.objectid
                typeNum = data.type
                offset = data.offset

                if data.type == BTRFS_ROOT_BACKREF_KEY:
                    info = buf.read(btrfs_root_ref)
                    nS = Structure((t.char, 'name', data.len - btrfs_root_ref.size))
                    nD = buf.read(nS)
                    name = nD.name
                    Volume.volumes[objectID]._addLink(
                        offset,
                        info,
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
                        objectID,
                        offset,
                        info,
                    )
                else:
                    buf.skip(data.len)
                    continue

            offset += 1

    def _getDevInfo(self):
        return self.DEV_INFO(devid=1, uuid="")

    def _getFSInfo(self):
        return self.FS_INFO()

    TREE_SEARCH = Control.IOWR(17, btrfs_ioctl_search_args)
    INO_LOOKUP = Control.IOWR(18, btrfs_ioctl_ino_lookup_args)
    DEV_INFO = Control.IOWR(30, btrfs_ioctl_dev_info_args)
    FS_INFO = Control.IOR(31, btrfs_ioctl_fs_info_args)

# BTRFS_IOC_SYNC = ioctl.IO(BTRFS_IOCTL_MAGIC, 8)
# BTRFS_IOC_CLONE = ioctl.IOW(BTRFS_IOCTL_MAGIC, 9, ioctl.INT_SIZE)

# define BTRFS_IOC_SET_RECEIVED_SUBVOL _IOWR(BTRFS_IOCTL_MAGIC, 37, \
#                 struct btrfs_ioctl_received_subvol_args)


#         ret = ioctl(fd, BTRFS_IOC_TREE_SEARCH, &args);

# BTRFS_IOC_TREE_SEARCH = IOWR(BTRFS_IOCTL_MAGIC, 17, btrfs_ioctl_search_args)

# define BTRFS_IOC_INO_LOOKUP _IOWR(BTRFS_IOCTL_MAGIC, 18, \
#                    struct btrfs_ioctl_ino_lookup_args)
# define BTRFS_IOC_SUBVOL_GETFLAGS _IOR(BTRFS_IOCTL_MAGIC, 25, t_u64)
# define BTRFS_IOC_DEV_INFO _IOWR(BTRFS_IOCTL_MAGIC, 30, \
#                  struct btrfs_ioctl_dev_info_args)
# define BTRFS_IOC_FS_INFO _IOR(BTRFS_IOCTL_MAGIC, 31, \
#                    struct btrfs_ioctl_fs_info_args)
# define BTRFS_IOC_INO_PATHS _IOWR(BTRFS_IOCTL_MAGIC, 35, \
#                     struct btrfs_ioctl_ino_path_args)

# BTRFS_IOC_DEV_INFO = IOWR(BTRFS_IOCTL_MAGIC, 30, btrfs_ioctl_dev_info_args)
# BTRFS_IOC_FS_INFO = IOR(BTRFS_IOCTL_MAGIC, 31, btrfs_ioctl_fs_info_args)


# struct btrfs_ioctl_received_subvol_args {
#     char    uuid[BTRFS_UUID_SIZE];  /* in */
#     t_u64   stransid;       /* in */
#     t_u64   rtransid;       /* out */
#     struct btrfs_ioctl_timespec stime; /* in */
#     struct btrfs_ioctl_timespec rtime; /* out */
#     t_u64   flags;          /* in */
#     t_u64   reserved[16];       /* in */
# };
