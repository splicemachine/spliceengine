
#
# Splice Machine sample spec file
#
Summary: Splice Machine binary components for MapR platform
Name: sample
Version: 1.0
Release: 1
License: GPL
Group: Applications/Sound
Source: ftp://ftp.gnomovision.com/pub/cdplayer/cdplayer-1.0.tgz
URL: http://www.gnomovision.com/cdplayer/cdplayer.html
Distribution: WSS Linux
Vendor: Splice Machine, Inc.
Packager: Murray Brown <mbrown@splicemachine.com>

%description
Splice Machine's scale-out database is the world
leader in the OLPP space.

%prep
# commands to do things to prepare files

%build
# commands to make binary files from source

%install
# commands to stage files for rpm

%files
# ownership listing

%pre
%post
%preun
%postun

%clean
# remove temp build files 
