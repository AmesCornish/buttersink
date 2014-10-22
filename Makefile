##############################################################################
# Makefile for buttersink project
#
# Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

.PHONY : all
all : makestamps/apt makestamps/pip version.txt

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
	rm -r build dist buttersink.egg-info || true

.PHONY : install
install : version.txt
	./setup.py build
	sudo ./setup.py install

.PHONY : clean
clean : clean_setup
	rm -r make || true

.PHONY : pypi
pypi : version.txt
	./setup.py bdist sdist upload

makestamps/source : $(shell git ls-files '*.py')
	touch $@

version.txt : .git/index .git/refs/tags makestamps/source
	if git describe --tags --dirty | grep -q dirty; then \
		echo $$(git describe --tags --dirty)-$$(date +"%m%d") > $@; \
	else \
		echo $$(git describe --tags) > $@; \
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
EXEC=sudo ./buttersink.py ${OPTS}
TEST_DIR=/mnt/butter/bs-test
TEST_BUCKET=butter-sink

# Count of 100K chunks:
# If this is more then 5Meg, it will trigger a multipart upload
TEST_DATA_COUNT=2
BS_LINE="\t1b40ccc7-f2cc-5e45-bca6-d74c6ffc31c4\t103350"

DEBUG_CODE=^[^\#]*logger\.setLevel\(|^theDebug = True|pudb

.PHONY : test_quick
test_quick : makestamps/test_code
	@echo "*** Quick tests passed ***"

makestamps/test_code : makestamps/source
	flake8 buttersink
	! grep -IE "${DEBUG_CODE}" $$(find -name '*.py')
	python -m doctest buttersink/ioctl.py
	touch $@

.PHONY : test_full
test_full : makestamps/test_restore makestamps/test_code
	@echo "*** All tests passed ***"

.SECONDARY : $(addprefix ${TEST_DIR}/snaps/,A B C)
makestamps/test_backup : $(addprefix ${TEST_DIR}/snaps/,A B C)
	@read -p "Please delete S3 test buckets, and press <enter> " FOO
	${EXEC} ${TEST_DIR}/snaps/ s3://${TEST_BUCKET}/test/
	touch $@

${TEST_DIR} :
	sudo btrfs sub create $@
	sudo chown $$USER:$$USER $@

${TEST_DIR}/snaps ${TEST_DIR}/restore : | ${TEST_DIR}
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
	sudo btrfs sub del ${TEST_DIR}/snaps/* 2>/dev/null || true
	sudo rm ${TEST_DIR}/snaps/* 2>/dev/null || true
	sudo btrfs sub del ${TEST_DIR}/restore/* 2>/dev/null || true
	sudo rm ${TEST_DIR}/restore/* 2>/dev/null || true
	sudo rm ${TEST_DIR}/*.dat ${TEST_DIR}/sha256sum.txt 2>/dev/null || true
endef

.PHONY : clean_test
clean_test :
	${CLEAN_TEST}
	sudo rm ${LOGFILE} || true
	rm makestamps/test_* || true

makestamps/test_restore : SNAP=C
makestamps/test_restore : makestamps/test_backup | ${TEST_DIR}/restore
	$(CLEAN_TEST)
	sudo sync
	${EXEC} s3://butter-sink/test/${SNAP} ${TEST_DIR}/restore/
	sudo sync
	# Check that not *all* snapshots were restored
	! ls -d $(addprefix ${TEST_DIR}/restore/,A B C)
	cd ${TEST_DIR}/restore/${SNAP}; sha256sum --check sha256sum.txt
	touch $@

.PHONY : test_list
test_list :
	${EXEC} s3://${TEST_BUCKET}/test/
	${EXEC} ${TEST_DIR}/snaps/
