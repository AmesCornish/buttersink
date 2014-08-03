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

btrfs_qgroup_status_item = Structure(
    (t.le64, 'version'),
    (t.le64, 'generation'),
    (t.le64, 'flags'),
    (t.le64, 'scan'),
    packed=True
)

btrfs_block_group_item = Structure(
    (t.le64, 'used'),
    (t.le64, 'chunk_objectid'),
    (t.le64, 'flags'),
    packed=True
)

btrfs_qgroup_info_item = Structure(
    (t.le64, 'generation'),
    (t.le64, 'referenced'),
    (t.le64, 'referenced_compressed'),
    (t.le64, 'exclusive'),
    (t.le64, 'exclusive_compressed'),
    packed=True
)

btrfs_qgroup_limit_item = Structure(
    (t.le64, 'flags'),
    (t.le64, 'max_referenced'),
    (t.le64, 'max_exclusive'),
    (t.le64, 'rsv_referenced'),
    (t.le64, 'rsv_exclusive'),
    packed=True
)

BTRFS_QUOTA_CTL_ENABLE = 1
BTRFS_QUOTA_CTL_DISABLE = 2
BTRFS_QUOTA_CTL_RESCAN__NOTUSED = 3

btrfs_ioctl_quota_ctl_args = Structure(
    (t.u64, 'cmd'),
    (t.u64, 'status'),
    packed=True
)

btrfs_ioctl_quota_rescan_args = Structure(
    (t.u64, 'flags'),
    (t.u64, 'progress'),
    (t.u64, 'reserved', 6, t.readBuffer),
    packed=True
)

BTRFS_IOCTL_MAGIC = 0x94

objectTypeKeys = {
    'zero': 0,
    'BTRFS_INODE_ITEM_KEY': 1,
    'BTRFS_INODE_REF_KEY': 12,
    'BTRFS_INODE_EXTREF_KEY': 13,
    'BTRFS_XATTR_ITEM_KEY': 24,
    'BTRFS_ORPHAN_ITEM_KEY': 48,
    'BTRFS_DIR_LOG_ITEM_KEY': 60,
    'BTRFS_DIR_LOG_INDEX_KEY': 72,
    'BTRFS_DIR_ITEM_KEY': 84,
    'BTRFS_DIR_INDEX_KEY': 96,
    'BTRFS_EXTENT_DATA_KEY': 108,
    'BTRFS_CSUM_ITEM_KEY': 120,
    'BTRFS_EXTENT_CSUM_KEY': 128,
    'BTRFS_ROOT_ITEM_KEY': 132,
    'BTRFS_ROOT_BACKREF_KEY': 144,
    'BTRFS_ROOT_REF_KEY': 156,
    'BTRFS_EXTENT_ITEM_KEY': 168,
    'BTRFS_METADATA_ITEM_KEY': 169,
    'BTRFS_TREE_BLOCK_REF_KEY': 176,
    'BTRFS_EXTENT_DATA_REF_KEY': 178,
    'BTRFS_EXTENT_REF_V0_KEY': 180,
    'BTRFS_SHARED_BLOCK_REF_KEY': 182,
    'BTRFS_SHARED_DATA_REF_KEY': 184,
    'BTRFS_BLOCK_GROUP_ITEM_KEY': 192,
    'BTRFS_DEV_EXTENT_KEY': 204,
    'BTRFS_DEV_ITEM_KEY': 216,
    'BTRFS_CHUNK_ITEM_KEY': 228,
    'BTRFS_BALANCE_ITEM_KEY': 248,
    'BTRFS_QGROUP_STATUS_KEY': 240,
    'BTRFS_QGROUP_INFO_KEY': 242,
    'BTRFS_QGROUP_LIMIT_KEY': 244,
    'BTRFS_QGROUP_RELATION_KEY': 246,
    'BTRFS_DEV_STATS_KEY': 249,
    'BTRFS_DEV_REPLACE_KEY': 250,
    'BTRFS_UUID_KEY_SUBVOL': 251,
    'BTRFS_UUID_KEY_RECEIVED_SUBVOL': 252,
    'BTRFS_STRING_ITEM_KEY': 253,
}

objectTypeNames = {v: k for (k, v) in objectTypeKeys.iteritems()}


BTRFS_FIRST_FREE_OBJECTID = 256
BTRFS_LAST_FREE_OBJECTID = (1 << 64) - 256
BTRFS_FIRST_CHUNK_TREE_OBJECTID = 256

BTRFS_ROOT_SUBVOL_RDONLY = (1 << 0)

BTRFS_ROOT_TREE_OBJECTID = 1
BTRFS_FS_TREE_OBJECTID = 5
BTRFS_QUOTA_TREE_OBJECTID = 8


class Volume(object):

    """ Represents a subvolume. """

    volumes = {}
    mounts = {}

    def __init__(self, rootid, generation, info):
        """ Initialize. """
        # logger.debug("Volume %d: %s", id, pretty(info))
        self.id = rootid  # id in BTRFS_ROOT_TREE_OBJECTID, also FS treeid for this volume
        self.original_gen = generation
        self.current_gen = info.generation
        # self.size = info.bytes_used
        self.readOnly = bool(info.flags & BTRFS_ROOT_SUBVOL_RDONLY)
        self.level = info.level
        self.uuid = info.uuid
        self.parent_uuid = info.parent_uuid
        self.received_uuid = info.received_uuid

        self.totalSize = None
        self.exclusiveSize = None

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
            path = Volume.volumes[dirTree].fullPath
            return path + ("/" if path[-1] != "/" else "") + dirPath + name
        return "/"

    @property
    def linuxPaths(self):
        """ Return full paths from linux root. """
        if self.fullPath in Volume.mounts:
            yield Volume.mounts[self.fullPath]
        for ((dirTree, dirID, dirSeq), (dirPath, name)) in self.links.items():
            for path in Volume.volumes[dirTree].linuxPaths:
                yield path + "/" + dirPath + name

    def __str__(self):
        """ String representation. """
        # logger.debug("%d %d %d", self.gen, self.info.generation, self.info.inode.generation)
        # logger.debug("%o %o", self.info.flags, self.info.inode.flags)
        return """%4d '%s' (level:%d gen:%d total:%d exclusive:%d%s)
        %s (parent:%s received:%s)
        %s%s""" % (
            self.id,
            # ", ".join([dirPath + name for (dirPath, name) in self.links.values()]),
            self.fullPath,
            self.level,
            self.current_gen,
            # self.size,
            self.totalSize,
            self.exclusiveSize,
            " ro" if self.readOnly else "",
            self.uuid,
            self.parent_uuid,
            self.received_uuid,
            "\n\t".join(self.linuxPaths),
            # "\n\t" + pretty(self.__dict__),
            "",
        )


class Control(ioctl.Control):

    """ A btrfs IOCTL. """

    magic = BTRFS_IOCTL_MAGIC


class FileSystem(ioctl.Device):

    """ Mounted file system descriptor for ioctl actions. """

    def __init__(self, path):
        """ Initialize. """
        super(FileSystem, self).__init__(path)

        self.volumes = None
        self.mounts = None

    @property
    def subvolumes(self):
        """ Subvolumes contained in this mount. """
        self.SYNC()
        self._getRoots()
        self._getMounts()
        self._getUsage()

        volumes = Volume.volumes.values()
        volumes.sort(key=(lambda v: v.fullPath))
        return volumes

    def rescanSizes(self):
        """ Zero and recalculate quota sizes to subvolume sizes will be correct. """
        self.QUOTA_CTL(cmd=BTRFS_QUOTA_CTL_ENABLE)
        self.QUOTA_RESCAN()
        logger.info("Waiting for btrfs rescan...")
        self.QUOTA_RESCAN_WAIT()

    def _getMounts(self):
        Volume.mounts = {}
        defaultSubvol = self.DEFAULT_SUBVOL().id
        if defaultSubvol == 0:
            defaultSubvol = ""
        else:
            defaultSubvol = Volume.volumes[defaultSubvol].fullPath.rstrip("/")
        with open("/etc/mtab") as mtab:
            for line in mtab:
                (dev, path, fs, opts, freq, passNum) = line.split()
                if fs != "btrfs":
                    continue
                opts = {
                    opt: value
                    for (opt, _, value) in
                    [o.partition("=") for o in opts.split(",")]
                }
                subvol = "/" + opts.get('subvol', defaultSubvol)
                Volume.mounts[subvol] = path
                logger.debug("%s: %s", subvol, path)

    Key = collections.namedtuple('Key', ('objectid', 'type', 'offset'))

    Key.first = Key(
        objectid=0,
        type=0,
        offset=0,
    )

    Key.last = Key(
        objectid=BTRFS_LAST_FREE_OBJECTID,
        type=t.max_u32,
        offset=t.max_u64,
    )

    Key.next = (lambda key: FileSystem.Key(key.objectid, key.type, key.offset + 1))

    def _walkTree(self, treeid):
        key = FileSystem.Key.first

        while True:
            # Returned objects seem to be monotonically increasing in (objectid, type, offset)
            # min and max values are *not* filters.
            result = self.TREE_SEARCH(
                key=dict(
                    tree_id=treeid,
                    min_type=key.type,
                    max_type=FileSystem.Key.last.type,
                    min_objectid=key.objectid,
                    max_objectid=FileSystem.Key.last.objectid,
                    min_offset=key.offset,
                    max_offset=FileSystem.Key.last.offset,
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
                yield (data, buf.view(data.len))

                key = FileSystem.Key(data.objectid, data.type, data.offset).next()
                buf.skip(data.len)

    def _getRoots(self):
        for (header, buf) in self._walkTree(BTRFS_ROOT_TREE_OBJECTID):
            if header.type == objectTypeKeys['BTRFS_ROOT_BACKREF_KEY']:
                data = Structure(
                    (btrfs_root_ref, 'ref'),
                    (t.char, 'name', header.len - btrfs_root_ref.size),
                ).read(buf)

                info = data.ref
                name = data.name

                directory = self.INO_LOOKUP(treeid=header.offset, objectid=info.dirid)

                logger.debug("%s: %s %s", name, pretty(info), pretty(directory))

                Volume.volumes[header.objectid]._addLink(
                    header.offset,
                    info.dirid,
                    info.sequence,
                    directory.name,
                    name,
                )
            elif header.type == objectTypeKeys['BTRFS_ROOT_ITEM_KEY']:
                if header.len == btrfs_root_item.size:
                    info = btrfs_root_item.read(buf)
                elif header.len == btrfs_root_item_v0.size:
                    info = btrfs_root_item_v0.read(buf)
                else:
                    assert False, header.len

                if ((header.objectid >= BTRFS_FIRST_FREE_OBJECTID and
                        header.objectid <= BTRFS_LAST_FREE_OBJECTID
                     ) or
                        header.objectid == BTRFS_FS_TREE_OBJECTID):
                    Volume(
                        header.objectid,
                        header.offset,
                        info,
                    )

    def _getUsage(self):
        for (header, buf) in self._walkTree(BTRFS_QUOTA_TREE_OBJECTID):
            logger.debug("%s %s", objectTypeNames[header.type], header)

            if header.type == objectTypeKeys['BTRFS_QGROUP_INFO_KEY']:
                data = btrfs_qgroup_info_item.read(buf)
                try:
                    vol = Volume.volumes[header.offset]
                    vol.totalSize = data.referenced
                    vol.exclusiveSize = data.exclusive
                except KeyError:
                    pass

            elif header.type == objectTypeKeys['BTRFS_QGROUP_LIMIT_KEY']:
                data = btrfs_qgroup_limit_item.read(buf)
            elif header.type == objectTypeKeys['BTRFS_QGROUP_RELATION_KEY']:
                data = None
            else:
                data = None

            logger.debug('%s', pretty(data))

    def _getDevInfo(self):
        return self.DEV_INFO(devid=1, uuid="")

    def _getFSInfo(self):
        return self.FS_INFO()

    volid_struct = Structure(
        (t.u64, 'id')
    )

    SYNC = Control.IO(8)
    TREE_SEARCH = Control.IOWR(17, btrfs_ioctl_search_args)
    INO_LOOKUP = Control.IOWR(18, btrfs_ioctl_ino_lookup_args)
    DEFAULT_SUBVOL = Control.IOW(19, volid_struct)
    DEV_INFO = Control.IOWR(30, btrfs_ioctl_dev_info_args)
    FS_INFO = Control.IOR(31, btrfs_ioctl_fs_info_args)
    QUOTA_RESCAN = Control.IOW(44, btrfs_ioctl_quota_rescan_args)
    QUOTA_RESCAN_WAIT = Control.IO(46)
    QUOTA_CTL = Control.IOWR(40, btrfs_ioctl_quota_ctl_args)

# define BTRFS_IOC_DEFAULT_SUBVOL _IOW(BTRFS_IOCTL_MAGIC, 19, __u64)
# define BTRFS_IOC_INO_PATHS _IOWR(BTRFS_IOCTL_MAGIC, 35, \
#                     struct btrfs_ioctl_ino_path_args)
# define BTRFS_IOC_LOGICAL_INO _IOWR(BTRFS_IOCTL_MAGIC, 36, \
#                     struct btrfs_ioctl_ino_path_args)
# define BTRFS_IOC_SET_RECEIVED_SUBVOL _IOWR(BTRFS_IOCTL_MAGIC, 37, \
#                 struct btrfs_ioctl_received_subvol_args)
# define BTRFS_IOC_SEND _IOW(BTRFS_IOCTL_MAGIC, 38, struct btrfs_ioctl_send_args)
# define BTRFS_IOC_QUOTA_RESCAN_STATUS _IOR(BTRFS_IOCTL_MAGIC, 45, \
#                    struct btrfs_ioctl_quota_rescan_args)
