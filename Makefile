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

makestamps/source : $(shell git ls-files)
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
EXEC=sudo ./buttersink.py ${OPTS}
TEST_DIR=/mnt/butter/bs-test
TEST_BUCKET=butter-sink

.PHONY : test_quick
test_quick : makestamps/test_code
	@echo "*** Quick tests passed"

DEBUG_CODE=^[^\#]*logger\.setLevel\(|^theDebug = True|pudb

makestamps/test_code : makestamps/source
	flake8 buttersink
	! grep -IE "${DEBUG_CODE}" $$(find -name '*.py')
	python -m doctest buttersink/ioctl.py
	touch $@

.PHONY : test_full
test_full : makestamps/test_restore makestamps/test_code
	@echo "*** All tests passed"

.INTERMEDIATE : $(addprefix ${TEST_DIR}/snaps/,A B C)
makestamps/test_backup : $(addprefix ${TEST_DIR}/snaps/,A B C)
	@read -p "Please delete S3 test buckets, and press <enter> " FOO
	${EXEC} file://${TEST_DIR}/snaps/ s3://${TEST_BUCKET}/test/
	touch $@

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

define CLEAN_TEST
	sudo sync
	sudo btrfs sub del ${TEST_DIR}/snaps/* || true
	sudo rm ${TEST_DIR}/snaps/* || true
	sudo btrfs sub del ${TEST_DIR}/restore/* || true
	sudo rm ${TEST_DIR}/restore/* || true
	sudo rm ${TEST_DIR}/*.dat ${TEST_DIR}/sha256sum.txt || true
endef

.PHONY : clean_test
clean_test :
	${CLEAN_TEST}
	rm makestamps/test_* || true

makestamps/test_restore : SNAP=B
makestamps/test_restore : makestamps/test_backup | ${TEST_DIR}/restore
	$(CLEAN_TEST)
	sudo sync
	${EXEC} s3://butter-sink/test/${SNAP} file://${TEST_DIR}/restore/
	sudo sync
	# Check that not *all* snapshots were restored
	! ls -d $(addprefix ${TEST_DIR}/restore/,A B C)
	cd ${TEST_DIR}/restore/${SNAP}; sha256sum --check sha256sum.txt
	touch $@

.PHONY : test_list
test_list :
	${EXEC} s3://${TEST_BUCKET}/test/
	${EXEC} file://${TEST_DIR}/snaps/
