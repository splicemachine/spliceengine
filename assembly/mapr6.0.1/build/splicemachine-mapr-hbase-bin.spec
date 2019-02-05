#
Summary: Splice Machine HBase binary components for MapR platform
Name: splicemachine-mapr-hbase-bin
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
Provides: splicemachine-hbase-binary

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
mkdir -p -m 755 %{buildroot}/opt/splice/SPLICEMACHINE-%{version}.mapr6.0.1.p1.13
ln -s /opt/splice/SPLICEMACHINE-%{version}.mapr6.0.1.p1.13/ %{buildroot}/opt/splice/default

mkdir -p -m 755 %{buildroot}/opt/splice/SPLICEMACHINE-%{version}.mapr6.0.1.p1.13/bin/
install -m 755 %{buildroot}/../../../splice1850-tarball/bin/sqlshell.sh                             %{buildroot}/opt/splice/SPLICEMACHINE-%{version}.mapr6.0.1.p1.13/bin/
install -m 755 %{buildroot}/../../../splice1850-tarball/scripts/start-hbase.sh                      %{buildroot}/opt/splice/SPLICEMACHINE-%{version}.mapr6.0.1.p1.13/bin/ 
install -m 755 %{buildroot}/../../../splice1850-tarball/scripts/stop-hbase.sh                       %{buildroot}/opt/splice/SPLICEMACHINE-%{version}.mapr6.0.1.p1.13/bin/

mkdir -p -m 755 %{buildroot}/opt/splice/SPLICEMACHINE-%{version}.mapr6.0.1.p1.13/lib/
install -m 644 %{buildroot}/../../../splice1850-tarball/lib/*.jar                                   %{buildroot}/opt/splice/SPLICEMACHINE-%{version}.mapr6.0.1.p1.13/lib/

mkdir -p -m 755 %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/logs/
mkdir -p -m 755 %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/bin/
install -m 755 %{buildroot}/../../../hbase-bespoke/bin/hbase                                    %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/bin/
install -m 755 %{buildroot}/../../../hbase-bespoke/bin/hbase-jruby                              %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/bin/
install -m 755 %{buildroot}/../../../hbase-bespoke/bin/*.sh                                     %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/bin/
install -m 755 %{buildroot}/../../../hbase-bespoke/bin/*.rb                                     %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/bin/
mkdir -p -m 750 %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/master/WEB-INF/
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/master/index.html              %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/master/
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/master/WEB-INF/web.xml         %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/master/WEB-INF/
mkdir -p -m 750 %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/regionserver/WEB-INF/
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/regionserver/index.html        %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/regionserver/
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/regionserver/WEB-INF/web.xml   %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/regionserver/WEB-INF/
mkdir -p -m 750 %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/rest/WEB-INF/
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/rest/index.html                %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/rest/
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/rest/WEB-INF/web.xml           %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/rest/WEB-INF/
mkdir -p -m 750 %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/static/hbase_logo.png          %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/static/hbase_logo_med.gif      %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/static/hbase_logo_small.png    %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/static/jumping-orca_*png       %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/
mkdir -p -m 750 %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/css/
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/static/css/*.css               %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/css/
mkdir -p -m 750 %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/fonts/
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/static/fonts/glyphicons*       %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/fonts/
mkdir -p -m 750 %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/js/
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/static/js/*.js                 %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/js/
mkdir -p -m 750 %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/thrift/WEB-INF/
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/thrift/index.html              %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/thrift/ 
install -m 640 %{buildroot}/../../../hbase-bespoke/hbase-webapps/thrift/WEB-INF/web.xml         %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/thrift/WEB-INF/
mkdir -p -m 755 %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/lib/
install -m 644 %{buildroot}/../../../hbase-bespoke/lib/*.jar                                    %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/lib/
ln -s /opt/splice/default/lib/javax.servlet-api-3.1.0.jar                                       %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/lib/servlet-api-2.5-6.1.14.jar
ln -s /opt/splice/default/lib/javax.servlet-api-3.1.0.jar                                       %{buildroot}/opt/mapr/hbase/hbase-1.1.8-splice/lib/servlet-api-2.5.jar

%files
%defattr(-,mapr,mapr)
%dir /opt/mapr/hbase/hbase-1.1.8-splice/bin
%dir /opt/mapr/hbase/hbase-1.1.8-splice/lib
%dir /opt/mapr/hbase/hbase-1.1.8-splice/logs

# re-generate this list by removing it and then capturing the error from rpmbuild
/opt/mapr/hbase/hbase-1.1.8-splice/bin/*.sh
/opt/mapr/hbase/hbase-1.1.8-splice/bin/draining_servers.rb
/opt/mapr/hbase/hbase-1.1.8-splice/bin/get-active-master.rb
/opt/mapr/hbase/hbase-1.1.8-splice/bin/hbase
/opt/mapr/hbase/hbase-1.1.8-splice/bin/hbase-jruby
/opt/mapr/hbase/hbase-1.1.8-splice/bin/hirb.rb
/opt/mapr/hbase/hbase-1.1.8-splice/bin/region_mover.rb
/opt/mapr/hbase/hbase-1.1.8-splice/bin/region_status.rb
/opt/mapr/hbase/hbase-1.1.8-splice/bin/shutdown_regionserver.rb
/opt/mapr/hbase/hbase-1.1.8-splice/bin/thread-pool.rb
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/master/WEB-INF/web.xml
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/master/index.html
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/regionserver/WEB-INF
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/regionserver/index.html
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/rest/WEB-INF/web.xml
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/rest/index.html
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/css/bootstrap-theme.css
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/css/bootstrap-theme.min.css
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/css/bootstrap.css
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/css/bootstrap.min.css
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/css/hbase.css
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/fonts/glyphicons-halflings-regular.eot
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/fonts/glyphicons-halflings-regular.svg
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/fonts/glyphicons-halflings-regular.ttf
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/fonts/glyphicons-halflings-regular.woff
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/hbase_logo.png
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/hbase_logo_med.gif
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/hbase_logo_small.png
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/js/bootstrap.js
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/js/bootstrap.min.js
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/js/jquery.min.js
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/js/tab.js
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/static/jumping-orca_rotated_12percent.png
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/thrift/WEB-INF/web.xml
/opt/mapr/hbase/hbase-1.1.8-splice/hbase-webapps/thrift/index.html
/opt/mapr/hbase/hbase-1.1.8-splice/lib/*.jar
/opt/splice/default
/opt/splice/SPLICEMACHINE-2.7.0.1850.mapr6.0.1.p1.13/bin/sqlshell.sh
/opt/splice/SPLICEMACHINE-2.7.0.1850.mapr6.0.1.p1.13/bin/start-hbase.sh
/opt/splice/SPLICEMACHINE-2.7.0.1850.mapr6.0.1.p1.13/bin/stop-hbase.sh
/opt/splice/SPLICEMACHINE-2.7.0.1850.mapr6.0.1.p1.13/lib/db-client-2.7.0.1850-SNAPSHOT.jar
/opt/splice/SPLICEMACHINE-2.7.0.1850.mapr6.0.1.p1.13/lib/db-drda-2.7.0.1850-SNAPSHOT.jar
/opt/splice/SPLICEMACHINE-2.7.0.1850.mapr6.0.1.p1.13/lib/db-shared-2.7.0.1850-SNAPSHOT.jar
/opt/splice/SPLICEMACHINE-2.7.0.1850.mapr6.0.1.p1.13/lib/db-tools-i18n-2.7.0.1850-SNAPSHOT.jar
/opt/splice/SPLICEMACHINE-2.7.0.1850.mapr6.0.1.p1.13/lib/db-tools-ij-2.7.0.1850-SNAPSHOT.jar
/opt/splice/SPLICEMACHINE-2.7.0.1850.mapr6.0.1.p1.13/lib/javax.servlet-api-3.1.0.jar
/opt/splice/SPLICEMACHINE-2.7.0.1850.mapr6.0.1.p1.13/lib/javax.ws.rs-api-2.0.1.jar
/opt/splice/SPLICEMACHINE-2.7.0.1850.mapr6.0.1.p1.13/lib/jython-standalone-2.5.3.jar
/opt/splice/SPLICEMACHINE-2.7.0.1850.mapr6.0.1.p1.13/lib/splice_machine-assembly-uber.jar
/opt/splice/SPLICEMACHINE-2.7.0.1850.mapr6.0.1.p1.13/lib/splice_machine-assembly-yarn-webproxy.jar
/opt/splice/SPLICEMACHINE-2.7.0.1850.mapr6.0.1.p1.13/lib/splicemachine-mapr6.0.1-2.2.1-mapr-1808_2.11-2.7.0.1850-SNAPSHOT.jar


%pre
%post
%preun
%postun

%changelog
* Sat Oct 20 2018 Murray Brown<mbrown@splicemachine.com
 - Working build scripts

* Thu Oct 11 2018 Murray Brown<mbrown@splicemachine.com>
 - Initial package attempt
