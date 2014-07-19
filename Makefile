##############################################################################
# Makefile for buttersink project
#
# Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

.PHONY : all
all : makestamps/apt makestamps/pip

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
install :
	./setup.py build
	sudo ./setup.py install

.PHONY : clean
clean : clean_setup
	rm -r make || true

.PHONY : pypi
pypi : 
	./setup.py bdist upload

##############################################################################
# Test cases

# To test:

#   make clean_test
#   make test_backup
#   make clean_test
#   make test_restore
  
# OPTS=--dry-run
# OPTS=--verbose
EXEC=sudo ./butter_sink.py ${OPTS}
TEST_DIR=/mnt/butter/bs-test
TEST_BUCKET=butter-sink

.PHONY : test_backup
test_backup : ${TEST_DIR}/snaps/A ${TEST_DIR}/snaps/B ${TEST_DIR}/snaps/C
	${EXEC} file://${TEST_DIR}/snaps s3://${TEST_BUCKET}/test/

${TEST_DIR} :
	sudo btrfs sub create $@
	sudo chown $$USER:$$USER $@

${TEST_DIR}/snaps ${TEST_DIR}/restore : | ${TEST_DIR}
	mkdir $@

${TEST_DIR}/snaps/% : | ${TEST_DIR}/snaps
	dd if=/dev/urandom of=${TEST_DIR}/$*.dat bs=100K count=1
	cd ${TEST_DIR}; sha256sum --binary *.dat > sha256sum.txt
	sudo btrfs fi sync ${TEST_DIR}
	sudo btrfs sub snap -r ${TEST_DIR} $@
	cd $@; sha256sum --check sha256sum.txt

.PHONY : clean_test
clean_test :
	sudo sync
	sudo btrfs sub del ${TEST_DIR}/snaps/* || true
	sudo btrfs sub del ${TEST_DIR}/restore/* || true
	sudo btrfs sub del ${TEST_DIR} || true

.PHONY : test_restore
test_restore : SNAP=B
test_restore : | ${TEST_DIR}/restore
	${EXEC} s3://butter-sink/test/${SNAP} file://${TEST_DIR}/restore
	sudo sync
	cd ${TEST_DIR}/restore/${SNAP}; sha256sum --check sha256sum.txt
	# diff -ur ${TEST_DIR}/snaps ${TEST_DIR}/restore || true

.PHONY : test_list
test_list :
	${EXEC} s3://${TEST_BUCKET}/test/
	${EXEC} file://${TEST_DIR}/snaps
