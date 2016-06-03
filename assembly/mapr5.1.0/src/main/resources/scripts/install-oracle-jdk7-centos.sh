#!/bin/bash

# Install Oracle JDK 7 for RedHat/CentOS from RPM.  This script needs to be run with sudo/root privileges.

# Select correct RPM based on 32/64bit system type
if [ `getconf LONG_BIT` = "64" ]
then
    RPM="jdk-7u72-linux-x64.rpm"
else
    RPM="jdk-7u72-linux-i586.rpm"
fi
URL="http://download.oracle.com/otn-pub/java/jdk/7u72-b14/"

yum -y install wget
wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" "${URL}${RPM}"
rpm -Uhv "${RPM}"
