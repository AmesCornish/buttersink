
.PHONY : all
all : apt.stamp

# OPTS=--dry-run --delete
EXEC=sudo ./butter_sink.py ${OPTS}
TEST_DIR=/mnt/butter/bs-test

apt.stamp : apt.txt
	sudo apt-get install $$(cat $<)
	touch $@

.PHONY : test1
test1 :
	${EXEC} file:///home/snaps s3://butter-sink/test1

.PHONY : test2
test2 : ${TEST_DIR}/snaps/A ${TEST_DIR}/snaps/B ${TEST_DIR}/snaps/C
	${EXEC} file://${TEST_DIR}/snaps s3://butter-sink/test2/

${TEST_DIR} :
	sudo btrfs sub create $@
	sudo chown $$USER:$$USER $@

${TEST_DIR}/snaps : | ${TEST_DIR}
	mkdir $@

${TEST_DIR}/snaps/% : | ${TEST_DIR}/snaps
	dd if=/dev/urandom of=${TEST_DIR}/$*.dat bs=1M count=10
	sha256sum --binary ${TEST_DIR}/*.dat > ${TEST_DIR}/sha256sum.txt
	sudo btrfs fi sync ${TEST_DIR}
	sudo btrfs sub snap -r ${TEST_DIR} $@
	sha256sum --check $@/sha256sum.txt

clean_test :
	sudo btrfs fi sync ${TEST_DIR}
	sudo btrfs sub del ${TEST_DIR}/snaps/*
	sudo btrfs sub del ${TEST_DIR}
