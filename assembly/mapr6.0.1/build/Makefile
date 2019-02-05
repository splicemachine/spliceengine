# Comments are allowed

RPMROOT=$(shell pwd)/rpmbuild
BUILDROOT=$(RPMROOT)/BUILDROOT

all: prep rpm

clean: 
	rm -rf ${BUILDROOT}

prep:
	mkdir -p ${RPMROOT}/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

rpm: *.spec
	rpmbuild --buildroot=${BUILDROOT} --target noarch-splicemachine-linux -bb splicemachine-mapr-master.spec
	rpmbuild --buildroot=$(BUILDROOT) --target noarch-splicemachine-linux -bb splicemachine-mapr-region.spec
	rpmbuild --buildroot=${BUILDROOT} --target noarch-splicemachine-linux -bb splicemachine-mapr-hbase-conf.spec
	rpmbuild --buildroot=${BUILDROOT} --target noarch-splicemachine-linux -bb splicemachine-mapr-hbase-secure-conf.spec
	rpmbuild --buildroot=${BUILDROOT} --target noarch-splicemachine-linux -bb splicemachine-mapr-hbase-bin.spec
