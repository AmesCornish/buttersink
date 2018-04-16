##############################################################################
# Makefile for buttersink project
#
# Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

.PHONY : all
all : makestamps/apt makestamps/pip buttersink/version.py

makestamps/apt : apt.txt | makestamps
 	if [ -f "/etc/debian_version" ]; then sudo apt-get install $$(cat $<) && touch $@; fi

mkaestamps/yum : yum.txt | makestamps
	if [ -f "/etc/redhat-release" ]; then sudo yum install -y $$(cat $<) && touch $@; fi
	
makestamps/pip : pip.txt | makestamps
	umask 22 && sudo -H pip install $$(cat $<)
	touch $@

makestamps :
	mkdir $@

.PHONY : clean_setup
clean_setup :
	./setup.py clean
	sudo rm -rf build dist buttersink.egg-info

.PHONY : install
install : buttersink/version.py
	./setup.py build
	sudo ./setup.py install

.PHONY : clean
clean : clean_setup
	rm -rf makestamps

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

# This is useful for testing with old versions of python
# PYTHONPATH=/usr/local/lib/python2.7/dist-packages:/usr/lib/python2.7/dist-packages
# PYTHON=/home/ames/python/bin/python
# EXEC=sudo -E PYTHONPATH=${PYTHONPATH} ${PYTHON} ./buttersink.py ${OPTS}

TEST_DIR=/mnt/butter/bs-test

# TEST_METHODS=ssh
TEST_METHODS=s3 ssh

TEST_REMOTE_HOST=proliant
TEST_REMOTE_ssh=ssh://bak@${TEST_REMOTE_HOST}/mnt/butter/bak/test
define CLEAN_REMOTE_ssh
	ssh root@${TEST_REMOTE_HOST} btrfs sub del -c '/mnt/butter/bak/test/*' || true
	ssh root@${TEST_REMOTE_HOST} rm '/mnt/butter/bak/test/*' || true
	ssh root@${TEST_REMOTE_HOST} mkdir -p '/mnt/butter/bak/test' || true
endef

TEST_REMOTE_s3=s3://butter-test/regress
define CLEAN_REMOTE_s3
	if which s3cmd; then \
		s3cmd rm ${TEST_REMOTE_s3} --recursive ; \
	else \
		read -p "Please delete S3 test buckets, and press <enter> " FOO ; \
	fi
endef

# Count of 100K chunks:
# If this is more then 5Meg, it will trigger a multipart upload
TEST_DATA_COUNT=2
BS_LINE="\t1b40ccc7-f2cc-5e45-bca6-d74c6ffc31c4\t103350"

RESTORE_SNAP=B


DEBUG_CODE=^[^\#]*logger\.setLevel\(|^theDebug = True|pudb

test : test_full ;
.PHONY : test

test_quick : makestamps/test_code
	@echo "*** Quick tests passed ***"
.PHONY : test_quick

test_full : test_restore test_quick
	@echo "*** All tests passed ***"
.PHONY : test_full

test_restore : $(addprefix makestamps/test_restore_, ${TEST_METHODS}) makestamps/test_backup
	@echo "*** Backup and restore tests passed ***"
.PHONY : test_restore

makestamps/test_backup : $(addprefix makestamps/test_backup_, ${TEST_METHODS})
	touch $@

makestamps/test_code : makestamps/source all
	flake8 buttersink
	python -m doctest buttersink/ioctl.py
	! grep -IE "${DEBUG_CODE}" $$(find -name '*.py')
	touch $@

.SECONDARY : $(addprefix ${TEST_DIR}/snaps/,A B C)

makestamps/test_backup_% : $(addprefix ${TEST_DIR}/snaps/,A B C)
	${CLEAN_REMOTE_$*}
	${EXEC} ${TEST_DIR}/snaps/ ${TEST_REMOTE_$*}/
	touch $@

${TEST_DIR} :
	sudo btrfs sub create $@
	sudo chown $$USER:$$USER $@

${TEST_DIR}/snaps ${TEST_DIR}/restore : | ${TEST_DIR}
	mkdir -p $@

${TEST_DIR}/snaps/% : | ${TEST_DIR}/snaps
	dd if=/dev/urandom of=${TEST_DIR}/$*.dat bs=100K count=${TEST_DATA_COUNT}
	cd ${TEST_DIR}; sha256sum --binary *.dat > sha256sum.txt
	sudo btrfs fi sync ${TEST_DIR}
	sudo btrfs sub snap -r ${TEST_DIR} $@
	${EXEC} -n $@ | awk '/^[-0-9a-z]{36} / { print $$1, ${BS_LINE}}' > $@.bs
	cd $@; sha256sum --check sha256sum.txt

define CLEAN_TEST
	sudo sync
	sudo btrfs sub del -c ${TEST_DIR}/*/* 2>/dev/null || true
	sudo rm ${TEST_DIR}/*/* 2>/dev/null || true
	sudo rm ${TEST_DIR}/*.dat ${TEST_DIR}/sha256sum.txt 2>/dev/null || true
endef

clean_test :
	${CLEAN_TEST}
	sudo rm ${LOGFILE} 2>/dev/null || true
	rm makestamps/test_* 2>/dev/null || true
	${CLEAN_REMOTE_ssh}
	${CLEAN_REMOTE_s3}
.PHONY : clean_test

makestamps/test_restore_% : makestamps/test_backup | ${TEST_DIR}/restore
	$(CLEAN_TEST)
	sudo sync
	${EXEC} ${TEST_REMOTE_$*}/${RESTORE_SNAP} ${TEST_DIR}/restore/
	sudo sync
	# Check that not *all* snapshots were restored
	! ls -d $(addprefix ${TEST_DIR}/restore/,A B C)
	cd ${TEST_DIR}/restore/${RESTORE_SNAP}; sha256sum --check sha256sum.txt
	$(CLEAN_TEST)
	touch $@

test_list :
	${EXEC} ${TEST_REMOTE_SSH}/
	${EXEC} ${TEST_REMOTE_S3}/
	${EXEC} ${TEST_DIR}/snaps/
.PHONY : test_list
