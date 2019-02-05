
Summary: Splice Machine HBase configuration files for Secure MapR platform
Name: splicemachine-mapr-hbase-secure-conf
Version: 2.7.0.1850
Release: 1
License: AGPL
Group: SpliceMachine
Distribution: Linux
BuildArch: noarch
#Source: http://repository.splicemachine.com/some-path-to/file.tgz
URL: https://doc.splicemachine.com/onprem_install_intro.html
Vendor: Splice Machine, Inc.
Packager: Murray Brown <mbrown@splicemachine.com>
Provides: splicemachine-hbase-secure-config
Conflicts: splicemachine-hbase-config
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
mkdir -p %{buildroot}/opt/mapr/conf
# /opt/mapr/conf/mapr-splice-hbase-config.sh
install -m 755 %{buildroot}/../../../templates/mapr-splice-hbase-config.sh   %{buildroot}/opt/mapr/conf/
mkdir -p %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/bin
install -m 755 %{buildroot}/../../../templates/hbase-daemon.sh               %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/bin/
mkdir -p %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/conf
install -m 644 %{buildroot}/../../../templates/hbase-site.secure.xml         %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/conf/hbase-site.xml
install -m 744 %{buildroot}/../../../templates/hbase-env.secure.sh           %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/conf/hbase-env.sh

%files
%config %attr(-, mapr, mapr) /opt/mapr/conf/mapr-splice-hbase-config.sh
%config %attr(-, mapr, mapr) /opt/mapr/hbase/hbase-1.1.8-splice/bin/hbase-daemon.sh
%config %attr(-, mapr, mapr) /opt/mapr/hbase/hbase-1.1.8-splice/conf/hbase-site.xml
%config %attr(-, mapr, mapr) /opt/mapr/hbase/hbase-1.1.8-splice/conf/hbase-env.sh

%pre
%post
%preun
%postun

%changelog
* Sat Oct 20 2018 Murray Brown<mbrown@splicemachine.com
 - Working build scripts

* Thu Oct 11 2018 Murray Brown<mbrown@splicemachine.com>
 - Initial package attempt
