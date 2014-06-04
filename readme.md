	butter_sync.py [options...] <src> <dest>

	--dry-run, -d:	Just print what it would do
	--delete:		Delete any snapshots in <dest> that are not in <src>

	--receive, -r:	Used to intelligently receive diffs
	--list, -l:		Show subvolumes and sizes

'batch', 'interactive', 'quiet', 'verbose'

	<src>, <dest>:	file://path/to/directory
					ssh://[user@]host/path/to/directory
					s3://bucket/prefix

	butter_sync synchronizes two sets of btrfs read-only subvolumes (snapshots).

	Kind of like rsync, but for btrfs subvolumes instead of files, which makes
	it much more efficient for things like archiving backup snapshots.  It is
	built on top of btrfs send and receive capabilities.  Sources and
	destinations can be local btrfs file systems, remote file systems over
	SSH, or S3 buckets.

	To use the ssh back-end, butter_sync should be installed on the remote system.

	butter_sync ignore read-write subvolumes and any files not in a subvolume.

	All options can be stored in a conf file, $HOME/butter_sync.conf by default.
