
.PHONY : all
all : apt.stamp

EXEC=./butter_sink.py

apt.stamp : apt.txt
	sudo apt-get install $$(cat $<)
	touch $@

.PHONY : test1
test1 :
	sudo ${EXEC} --dry-run file:///home/snaps s3://butter-sink/test1
