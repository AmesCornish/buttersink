About
=====

ButterSink synchronizes two sets of btrfs read-only subvolumes
(snapshots).

ButterSink is like rsync, but for btrfs subvolumes instead of files,
which makes it much more efficient for things like archiving backup
snapshots. It is built on top of btrfs send and receive capabilities.
Sources and destinations can be local btrfs file systems, remote btrfs
file systems over SSH, or S3 buckets.

To use the ssh back-end, ButterSink must be installed on the remote
system.

ButterSink *only* handles read-only subvolumes. It ignores read-write
subvolumes and any files not in a subvolume.

Features
========

ButterSink is designed for efficient reliable transfers for backups.
Currently implemented features include:

  * Transfers between a local btrfs filesystem, Amazon S3 bucket, other local btrfs filesystems, or remote btrfs filesystems over ssh

  * Automatically synchronizes a set of snapshots, or a single snapshot,
transferring only needed differences

  * Intelligent selection of full and incremental transfers to minimize costs
of transfer and storage, and to minimize risks from corruption of a difference.

    * Smart heuristics based on S3 file sizes, btrfs quota information, and btrfs-tools internal snapshot parent identification ("ruuid")
    
    * Will measure actual size of candidate diff before remote transfers.

  * Robust handling of btrfs send and receive errors

    * Detects and (optionally) deletes failed partial transfers
    
    * Resumable, checksummed multi-part uploads to S3 as a back-end
    
    * Adjusts sent UUID so that restored snapshots can also be used for new diffs.

  * Conveniently lists snapshots and sizes in either btrfs or S3

Usage
=====

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
usage: buttersink.py [-h] [-n] [-d] [-e] [-q] [-l LOGFILE] [-V]
                     [--part-size PART_SIZE]
                     [<src>] <dst>

Synchronize two sets of btrfs snapshots.

positional arguments:
  <src>                 a source of btrfs snapshots
  <dst>                 the btrfs snapshots to be updated

optional arguments:
  -h, --help            show this help message and exit
  -n, --dry-run         display what would be transferred, but don't do it
  -d, --delete          delete any snapshots in <dst> that are not in <src>
  -e, --estimate        use estimated size instead of measuring diffs with a
                        local test send
  -q, --quiet           once: don't display progress. twice: only display
                        error messages
  -l LOGFILE, --logfile LOGFILE
                        log debugging information to file
  -V, --version         display version
  --part-size PART_SIZE
                        Size of chunks in a multipart upload

<src>, <dst>:   [btrfs://]/path/to/directory/[snapshot]
                s3://bucket/prefix/[snapshot]
                ssh://[user@]host/path/to/directory/[snapshot]

If only <dst> is supplied, just list available snapshots.  NOTE: The trailing
"/" *is* significant.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

S3 Authentication
=================

S3 interaction and S3 authentication are handled by Boto. Boto will read
S3 credentials from `~/.boto`, which should look like this:

    [Credentials]
    aws_access_key_id=AKIAIOSFODNN7EXAMPLE
    aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

AWS access policies are tricky. Here's an example policy to give an IAM
user access for ButterSink:

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

SSH Authentication
==================

If the source or destination is an ssh://user@host/path url, then a connection is made to the remote host using a command line like:

    ssh user@host sudo buttersink --server --mode w /mnt/butter/

Modes are:

    'r' -- just downloads
    'a' -- uploads
    'w' -- uploads with --delete

Notice that sudo is used to ensure root privileges, which are required by btrfs.  The user should be root, or sudo should be configured to a allow the user to issue the buttersink command without a password.  Here's an example entry in /etc/sudoers for user fred, giving access to snapshots in /bak:

    fred ALL = NOPASSWD: /usr/local/bin/buttersink --server --mode r /bak/*
    fred ALL = NOPASSWD: /usr/local/bin/buttersink --server --mode a /bak/*
    fred ALL = NOPASSWD: /usr/local/bin/buttersink --server --mode w /bak/*

Installation
============

From source:

    git clone https://github.com/AmesCornish/buttersink.git
    cd buttersink
    make
    ./buttersink.py --help

    sudo make install
    buttersink --help

With PyPi:

    pip install --upgrade buttersink
    buttersink --help

Utilities
=========

    checksumdir <dir>

Checksumdir is a utility to create checksum hashes of all the data and key
metadata in a directory.  Useful for verifying the integrity of backups.

    sudo btrfslist <dir>
    
Btrfslist will show snapshots, including their size and UUID.  Like `btrfs subvolume list`, but with more detail.
    
Contact
=======

    Ames Cornish
    buttersink@montebellopartners.com
    https://github.com/AmesCornish/buttersink/wiki

Copyright (c) 2014 Ames Cornish. All rights reserved. Licensed under
GPLv3.

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation; either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See LICENSE.txt for more details.
