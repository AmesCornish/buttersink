# Usage

usage: butter_sink.py [-h] [-n] [-d] [-r] [-b] [-q] [<src>] <dst>

Synchronize two sets of btrfs snapshots.

positional arguments:
  <src>          a source of btrfs snapshots
  <dst>          the btrfs snapshots to be updated

optional arguments:
  -h, --help     show this help message and exit
  -n, --dry-run  display what would be transferred, but don't do it
  -d, --delete   delete any snapshots in <dst> that are not in <src> (Not implemented)
  -r, --receive  internal command to intelligently receive diffs (Not implemented)
  -b, --batch    non-interactive (Not implemented)
  -q, --quiet    only display error messages (Not implemented)

<src>, <dst>:   file:///path/to/directory
                ssh://[user@]host//path/to/directory (Not implemented)
                s3://bucket/prefix

If only <dst> is supplied, just list available snapshots.

# About

butter_sink synchronizes two sets of btrfs read-only subvolumes (snapshots).

butter_sink is like rsync, but for btrfs subvolumes instead of files, which
makes it much more efficient for things like archiving backup snapshots.  It is
built on top of btrfs send and receive capabilities.  Sources and destinations
can be local btrfs file systems, remote btrfs file systems over SSH, or S3 buckets.

To use the ssh back-end, butter_sink must be installed on the remote system.

butter_sink *only* handles read-only subvolumes.  It ignores read-write subvolumes and any files not in a subvolume.

<del>All options can be stored in a configuration file, $HOME/butter_sink.conf by default.</del>

# Authentication

S3 interaction and S3 authentication are handled by boto.  Boto will read S3 credentials from `~/.boto`, which should look like this:

    [Credentials]
    aws_access_key_id=AKIAIOSFODNN7EXAMPLE
    aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

AWS access policies are tricky.  Here's an example policy to give an IAM user access for butter_sink:

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

# Installation

    git clone xxx
    cd butter_sink
    make
    ./butter_sink.py --help

# Authors

# Contact

# Testing

    make test1
    make test2
    make test3

