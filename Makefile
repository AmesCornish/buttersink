##############################################################################
# Makefile for buttersink project
#
# Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

.PHONY : all
all : makestamps/apt makestamps/pip buttersink/version.py

makestamps/apt : apt.txt | makestamps
	sudo apt-get install $$(cat $<)
	touch $@

makestamps/pip : pip.txt | makestamps
	sudo pip install $$(cat $<)
	touch $@

makestamps :
	mkdir $@

.PHONY : clean_setup
clean_setup :
	./setup.py clean
	sudo rm -r build dist buttersink.egg-info || true

.PHONY : install
install : buttersink/version.py
	./setup.py build
	sudo ./setup.py install

.PHONY : clean
clean : clean_setup
	rm -r make || true

.PHONY : pypi
pypi : buttersink/version.py
	./setup.py bdist sdist upload

makestamps/source : $(shell git ls-files '*.py')
	touch $@

buttersink/version.py : .git/index .git/refs/tags makestamps/source
	if git describe --tags --dirty | grep -q dirty; then \
		echo "version = \"$$(git describe --tags --dirty)-$$(date +%m%d)\"" > $@; \
	else \
		echo "version = \"$$(git describe --tags)\"" > $@; \
	fi
	cat $@

##############################################################################
# Test cases

# To test:

#   make test_full
  
# OPTS=--dry-run
# OPTS=--verbose
LOGFILE=make_test.log
OPTS:=-l ${LOGFILE}
OPTS:=${OPTS} --part-size=5
EXEC=sudo -E ./buttersink.py ${OPTS}

TEST_DIR=/mnt/butter/bs-test

TEST_REMOTE_SSH=ssh://bak@proliant/mnt/butter/bak/test
define CLEAN_REMOTE_SSH
	ssh root@proliant btrfs sub del -c '/mnt/butter/bak/test/*' || true
	ssh root@proliant rm '/mnt/butter/bak/test/*' || true
endef
TEST_RESTORE_SSH=${TEST_DIR}/restore_ssh

TEST_REMOTE_S3=s3://butter-sink/test
define CLEAN_REMOTE_S3
	if which s3cmd; then \
		s3cmd rm ${TEST_REMOTE_S3} --recursive ; \
	else \
		read -p "Please delete S3 test buckets, and press <enter> " FOO ; \
	fi
endef
TEST_RESTORE_S3=${TEST_DIR}/restore_s3

# Count of 100K chunks:
# If this is more then 5Meg, it will trigger a multipart upload
TEST_DATA_COUNT=2
BS_LINE="\t1b40ccc7-f2cc-5e45-bca6-d74c6ffc31c4\t103350"

RESTORE_SNAP=B


DEBUG_CODE=^[^\#]*logger\.setLevel\(|^theDebug = True|pudb

test_quick : makestamps/test_code
	@echo "*** Quick tests passed ***"
.PHONY : test_quick

test_restore : makestamps/test_restore_s3 makestamps/test_restore_ssh
	@echo "*** Backup and restore tests passed ***"
.PHONY : test_full

test_full : test_restore test_quick
	@echo "*** All tests passed ***"
.PHONY : test_full

makestamps/test_code : makestamps/source
	flake8 buttersink
	python -m doctest buttersink/ioctl.py
	! grep -IE "${DEBUG_CODE}" $$(find -name '*.py')
	touch $@

.SECONDARY : $(addprefix ${TEST_DIR}/snaps/,A B C)

makestamps/test_backup_ssh makestamps/test_restore_ssh : TEST_REMOTE=${TEST_REMOTE_SSH}
makestamps/test_backup_ssh makestamps/test_restore_ssh : TEST_RESTORE=${TEST_RESTORE_SSH}
makestamps/test_backup_ssh makestamps/test_restore_ssh : CLEAN_REMOTE=${CLEAN_REMOTE_SSH}
makestamps/test_backup_s3 makestamps/test_restore_s3 : TEST_REMOTE=${TEST_REMOTE_S3}
makestamps/test_backup_s3 makestamps/test_restore_s3 : TEST_RESTORE=${TEST_RESTORE_S3}
makestamps/test_backup_s3 makestamps/test_restore_s3 : CLEAN_REMOTE=${CLEAN_REMOTE_S3}

makestamps/test_backup_ssh makestamps/test_backup_s3 : $(addprefix ${TEST_DIR}/snaps/,A B C)
	${CLEAN_REMOTE}
	${EXEC} ${TEST_DIR}/snaps/ ${TEST_REMOTE}/
	touch $@

${TEST_DIR} :
	sudo btrfs sub create $@
	sudo chown $$USER:$$USER $@

${TEST_DIR}/snaps ${TEST_RESTORE_S3} ${TEST_RESTORE_SSH} : | ${TEST_DIR}
	mkdir $@

${TEST_DIR}/snaps/% : | ${TEST_DIR}/snaps
	dd if=/dev/urandom of=${TEST_DIR}/$*.dat bs=100K count=${TEST_DATA_COUNT}
	cd ${TEST_DIR}; sha256sum --binary *.dat > sha256sum.txt
	sudo btrfs fi sync ${TEST_DIR}
	sudo btrfs sub snap -r ${TEST_DIR} $@
	${EXEC} -n $@ | awk '/^[-0-9a-z]{36} / { print $$1, ${BS_LINE}}' > $@.bs
	cd $@; sha256sum --check sha256sum.txt

define CLEAN_TEST
	sudo sync
	sudo btrfs sub del -c ${TEST_DIR}/snaps/* 2>/dev/null || true
	sudo rm ${TEST_DIR}/snaps/* 2>/dev/null || true
	sudo btrfs sub del -c ${TEST_RESTORE}/* 2>/dev/null || true
	sudo rm ${TEST_RESTORE}/* 2>/dev/null || true
	sudo rm ${TEST_DIR}/*.dat ${TEST_DIR}/sha256sum.txt 2>/dev/null || true
endef

.PHONY : clean_test
clean_test :
	${CLEAN_TEST}
	sudo rm ${LOGFILE} || true
	rm makestamps/test_* || true
	${CLEAN_REMOTE_SSH}
	${CLEAN_REMOTE_S3}

makestamps/test_restore_ssh makestamps/test_restore_s3 : makestamps/test_restore_% : makestamps/test_backup_% | ${TEST_DIR}/restore_% makestamps/test_backup_s3 makestamps/test_backup_ssh
	$(CLEAN_TEST)
	sudo sync
	${EXEC} ${TEST_REMOTE}/${RESTORE_SNAP} ${TEST_RESTORE}/
	sudo sync
	# Check that not *all* snapshots were restored
	! ls -d $(addprefix ${TEST_RESTORE}/,A B C)
	cd ${TEST_RESTORE}/${RESTORE_SNAP}; sha256sum --check sha256sum.txt
	touch $@

.PHONY : test_list
test_list :
	${EXEC} ${TEST_REMOTE_SSH}/
	${EXEC} ${TEST_REMOTE_S3}/
	${EXEC} ${TEST_DIR}/snaps/
