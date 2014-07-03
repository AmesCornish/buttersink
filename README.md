# About

ButterSink synchronizes two sets of btrfs read-only subvolumes (snapshots).

ButterSink is like rsync, but for btrfs subvolumes instead of files, which
makes it much more efficient for things like archiving backup snapshots.  It is
built on top of btrfs send and receive capabilities.  Sources and destinations
can be local btrfs file systems, remote btrfs file systems over SSH, or S3 buckets.

To use the ssh back-end, ButterSink must be installed on the remote system.

ButterSink *only* handles read-only subvolumes.  It ignores read-write subvolumes and any files not in a subvolume.

# Usage

usage: buttersink.py [-h] [-n] [-d] [-q] [-l LOGFILE] [-V] [<src>] <dst>

Synchronize two sets of btrfs snapshots.

positional arguments:
  <src>                 a source of btrfs snapshots
  <dst>                 the btrfs snapshots to be updated

optional arguments:
  -h, --help            show this help message and exit
  -n, --dry-run         display what would be transferred, but don't do it
  -d, --delete          delete any snapshots in <dst> that are not in <src>
  -q, --quiet           once: don't display progress. twice: only display
                        error messages
  -l LOGFILE, --logfile LOGFILE
                        log debugging information to file
  -V, --version         display version

<src>, <dst>:   file:///path/to/directory
                ssh://[user@]host/path/to/directory (Not implemented)
                s3://bucket/prefix[/snapshot]

If only <dst> is supplied, just list available snapshots.

# Authentication

S3 interaction and S3 authentication are handled by Boto.  Boto will read S3 credentials from `~/.boto`, which should look like this:

    [Credentials]
    aws_access_key_id=AKIAIOSFODNN7EXAMPLE
    aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

AWS access policies are tricky.  Here's an example policy to give an IAM user access for ButterSink:

    {
      "Statement": [
        {
          "Effect": "Allow",
          "Action": ["s3:*"],
          "Resource": [
            "arn:aws:s3:::myBackupBucketName",
            "arn:aws:s3:::myBackupBucketName/*"
          ]
        }
      ]
    }

ButterSink needs root privileges to access btrfs file systems.

# Installation

From source:

    git clone https://github.com/AmesCornish/buttersink.git
    cd buttersink
    make
    ./buttersink.py --help

With PyPi:

    pip install buttersink
    buttersink --help

# Contact

    Ames Cornish
    buttersink@montebellopartners.com
    https://github.com/AmesCornish/buttersink/wiki

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; either version 3 of the License, or (at your option) any later
version.
                                                                              
This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.
                                                                              
See LICENSE.md for more details.
