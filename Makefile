
.PHONY : all
all : apt.stamp

# OPTS=--dry-run
# OPTS=--verbose
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

${TEST_DIR}/snaps ${TEST_DIR}/restore : | ${TEST_DIR}
	mkdir $@

${TEST_DIR}/snaps/% : | ${TEST_DIR}/snaps
	dd if=/dev/urandom of=${TEST_DIR}/$*.dat bs=1M count=1
	cd ${TEST_DIR}; sha256sum --binary *.dat > ${TEST_DIR}/sha256sum.txt
	sudo btrfs fi sync ${TEST_DIR}
	sudo btrfs sub snap -r ${TEST_DIR} $@
	sha256sum --check $@/sha256sum.txt

clean_test :
	sudo btrfs fi sync ${TEST_DIR}
	sudo btrfs sub del ${TEST_DIR}/snaps/* || true
	sudo btrfs sub del ${TEST_DIR}/restore/* || true
	sudo btrfs sub del ${TEST_DIR} || true

.PHONY : test3
test3 : | ${TEST_DIR}/restore
	sudo btrfs sub del ${TEST_DIR}/restore/* || true
	${EXEC} s3://butter-sink/test2/B file://${TEST_DIR}/restore
	sha256sum --check ${TEST_DIR}/restore/*/sha256sum.txt
	diff -ur ${TEST_DIR}/snaps ${TEST_DIR}/restore || true

.PHONY : list
list :
	${EXEC} s3://butter-sink/test2/
	${EXEC} file://${TEST_DIR}/snaps
