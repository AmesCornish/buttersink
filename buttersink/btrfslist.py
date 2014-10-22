#! /usr/bin/python

""" Utility program to list data about btrfs subvolumes.

Gives more information than "btrfs sub list", but doesn't filter or format.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.
"""

if True:  # imports

    import argparse
    import logging
    import pprint
    import sys

    import btrfs


theDisplayFormat = '%(message)s'
theDebugDisplayFormat = (
    '%(levelname)7s:'
    '%(filename)s[%(lineno)d] %(funcName)s(): %(message)s'
)

logging.basicConfig(format=theDebugDisplayFormat, level='INFO')
logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')

command = argparse.ArgumentParser(
    description="List data about btrfs subvolumes.",
    epilog="""

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.
See README.md and LICENSE.txt for more info.
    """,
    formatter_class=argparse.RawDescriptionHelpFormatter,
)

command.add_argument('dir', metavar='<dir>',
                     help='list subvolumes in this directory')


def main():
    """ Main program. """
    args = command.parse_args()

    with btrfs.FileSystem(args.dir) as mount:
        # mount.rescanSizes()

        fInfo = mount.FS_INFO()
        pprint.pprint(fInfo)

        vols = mount.subvolumes

        # for dev in mount.devices:
        #     pprint.pprint(dev)

        for vol in vols:
            print vol

    return 0

if __name__ == "__main__":
    sys.exit(main())
