""" Pythonic wrapper around kernel ioctl-level access to btrfs. """

# See <linux/btrfs.h> for C source.
# See btrfs-progs/ctree.h for some constants.
# Headers taken from 3.13.0-32

# This is NOT a complete implementation,
# just a few useful routines for my current project.

from ioctl import t
import ioctl

BTRFS_DEVICE_PATH_NAME_MAX = 1024
BTRFS_SUBVOL_CREATE_ASYNC = (1 << 0)
BTRFS_SUBVOL_RDONLY = (1 << 1)
BTRFS_SUBVOL_QGROUP_INHERIT = (1 << 2)
BTRFS_FSID_SIZE = 16
BTRFS_UUID_SIZE = 16

btrfs_ioctl_search_key = (
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
)

btrfs_ioctl_search_header = (
    (t.u64, 'transid'),
    (t.u64, 'objectid'),
    (t.u64, 'offset'),
    (t.u32, 'type'),
    (t.u32, 'len'),
)

btrfs_ioctl_dev_info_args = (
    (t.u64,  'devid'),                # /* in/out */
    (t.u8(BTRFS_UUID_SIZE),  'uuid'),     # /* in/out */
    (t.u64,  'bytes_used'),           # /* out */
    (t.u64,  'total_bytes'),          # /* out */
    (t.pad_64(379),  'unused'),          # /* pad to 4k */
    (t.u8(BTRFS_DEVICE_PATH_NAME_MAX),  'path'),  # /* out */
)

btrfs_ioctl_fs_info_args = (
    (t.u64,  'max_id'),               # /* out */
    (t.u64,  'num_devices'),          # /* out */
    (t.u8(BTRFS_FSID_SIZE),  'fsid'),     # /* out */
    (t.pad_64(124),  'reserved'),            # /* pad to 1k */
)


BTRFS_SEARCH_ARGS_BUFSIZE = (4096 - ioctl.sizeof(btrfs_ioctl_search_key))

btrfs_ioctl_search_args = (
    (btrfs_ioctl_search_key, 'key'),
    (t.char(BTRFS_SEARCH_ARGS_BUFSIZE), 'buf'),
)

BTRFS_IOCTL_MAGIC = 0x94

BTRFS_ROOT_ITEM_KEY = 132
BTRFS_ROOT_BACKREF_KEY = 144
BTRFS_ROOT_REF_KEY = 156
BTRFS_FIRST_FREE_OBJECTID = 256
BTRFS_LAST_FREE_OBJECTID = (1 << 64)-256
BTRFS_FIRST_CHUNK_TREE_OBJECTID = 256


class Volume:

    """ Represents a subvolume. """

    def __init__(self):
        """ Initialize. """
        pass


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
        return self.volumes

    def _getTree(self):
        return self.TREE_SEARCH(
            key=dict(
                tree_id=1,
                max_type=BTRFS_ROOT_BACKREF_KEY,
                min_type=BTRFS_ROOT_ITEM_KEY,
                min_objectid=BTRFS_FIRST_FREE_OBJECTID,
                max_objectid=BTRFS_LAST_FREE_OBJECTID,
                max_offset=t.max_u64,
                max_transid=t.max_u64,
                nr_items=4096,
            ),
        )

    def _getDevInfo(self):
        return self.DEV_INFO(devid=1, uuid="")

    def _getFSInfo(self):
        return self.FS_INFO()

    TREE_SEARCH = Control.IOWR(17, btrfs_ioctl_search_args)
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
