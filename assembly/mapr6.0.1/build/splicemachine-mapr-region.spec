
Summary: Splice Machine binary components for MapR platform
Name: splicemachine-mapr-region
Version: 2.7.0.1834
Release: 1
License: AGPL
Group: SpliceMachine
Distribution: Linux
BuildArch: noarch
#Source: http://repository.splicemachine.com/some-path-to/file.tgz
URL: https://doc.splicemachine.com/onprem_install_intro.html
Vendor: Splice Machine, Inc.
Packager: Murray Brown <mbrown@splicemachine.com>
Provides: splicemachine-region-script
Requires: splicemachine-hbase-binary

%description
Splice Machine, the only Hadoop RDBMS, is designed to scale real-time applications
using commodity hardware without application rewrites. The Splice Machine database is a
modern, scale-out alternative to traditional RDBMSs, such as Oracle®, MySQL™, IBM DB2®
and Microsoft SQL Server®, that can deliver over a 10x improvement in price/performance.
As a full-featured SQL-on-Hadoop RDBMS with ACID transactions, the Splice Machine
database helps customers power real-time applications and operational analytics,
especially as they approach big data scale.

Splice Machine's scale-out database is the world leader in the OLPP space.

%prep
# no prep required

%clean
rm -r %{buildroot}

%build
mkdir -p %{buildroot}

%install
# commands to stage files for rpm
mkdir -p %{buildroot}/etc/rc.d/init.d/
install -m 755 %{buildroot}/../../../templates/splice-init-script %{buildroot}/etc/rc.d/init.d/splice-regionserver

%files
%attr(-, root, root) /etc/rc.d/init.d/splice-regionserver

%pre
%post
chkconfig --add splice-regionserver

%preun
%postun

%changelog
* Sat Oct 20 2018 Murray Brown<mbrown@splicemachine.com
 - Working build scripts

* Thu Oct 11 2018 Murray Brown<mbrown@splicemachine.com>
 - Initial package attempt
