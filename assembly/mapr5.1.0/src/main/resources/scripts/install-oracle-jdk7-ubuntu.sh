#!/bin/bash

# Install Oracle JDK 7 for Ubuntu from webupda8 PPA.  This script needs to be run with sudo/root privileges.

apt-get install -y python-software-properties
add-apt-repository -y ppa:webupd8team/java

# Accept licenses automatically
echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections

apt-get update
apt-get -y install oracle-java7-installer
apt-get -y install oracle-java7-set-default
