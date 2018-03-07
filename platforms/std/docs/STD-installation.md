# Installing the Standalone Version of Splice Machine

This topic walks you through downloading, installing, and getting
started with using the standalone version of Splice Machine. Follow these steps:

* [Prepare for Installation on Your Computer](#prepare-for-installation-on-your-computer)
* [Install Splice Machine](#install-splice-machine)
* [Start Using Splice Machine](#start-using-splice-machine)


## Prepare for Installation on Your Computer

This section walks you through preparing your computer for installation
of Splice Machine; follow the instructions for your operating system:

* [Mac OSX](#configure-mac-osx-for-splice-machine)
* [Ubuntu Linux](#configure-ubuntu-linux-for-splice-machine)
* [CentOS/Red Hat Enterprise Linux (RHEL)](#configure-centos/red-hat-enterprise-linux-(rhel)-for-splice-machine)

## Configure Mac OSX for Splice Machine

Follow these steps to prepare your MacOS computer to work with Splice
Machine:

1. If you have Homebrew installed:

   Fire up the `Terminal` app and enter the following commands:

   ````
   $ brew update
   $ brew cask install java
   $ brew install rlwrap
   ````

2. If you do not have Homebrew installed:

   a. Download the Java SE Development Kit (JDK 8) from this URL:    http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html

   b. Open the downloaded installer and compete the installation wizard to install the JDK.

   c. Download and install `rlWrap`. You can find rlWrap online, along with instructions for installing it using `brew` or another
 installation method. Some experts advise using Homebrew instead of any other method.

3. Check that your `$JAVA_HOME` environment variable is set:

   a. Edit your `~/.bash_profile` file with your favorite text editor;
      for example:

      ````
      $ sudo vi ~/.bash_profile
      ````

   b. Add the following `export` command at the bottom of the `.bash_profile` file.

      ````
      export JAVA_HOME=`/usr/libexec/java_home`
      ````

   c. Save the file and close your editor.

   d. Run the following command to load the updated profile into your current session:

      ````
      $ source ~/.bash_profile
      ````

   e. To verify that this setting is correct, make sure that the following commands display the same value:

      ````
      $ echo $JAVA_HOME/usr/libexec/java_home
      ````

### Configure Ubuntu Linux for Splice Machine

Follow these steps to prepare your Ubuntu computer to work with Splice
Machine:

  *NOTE:* Installing on Linux can be a bit tricky, so please use the `pwd` command
to ensure that you are in the directory in which you need to be.

1. Install the Java SE Development Kit

   Fire up the `Terminal` app and enter the following commands:

   ````
   $ sudo add-apt-repository ppa:webupd8team/java$ sudo apt-get update$ sudo apt-get install oracle-java8-installer
   $ sudo apt install oracle-java8-set-default
   ````

2. Check that your `$JAVA_HOME` environment variable is set:

   a. Find the location of Java on your computer by executing this command:

      ````
      $ sudo update-alternatives --config java
      ````

   b. Copy the resulting path to the clipboard.

   c. Use your favorite text editor to open `/etc/environment`:

      ````
      sudo vi /etc/environment
      ````

   d. Add the following `export` command at the bottom of the `/etc/environment` file.

      ````
      JAVA_HOME="/usr/lib/jvm/java-8-oracle"
      ````

   e. Run the following command to load the updated profile into your current session:

      ````
      source ~/etc/environment
      ````

   f. Run this command to make sure the setting is correct:

      ````
      $ echo $JAVA_HOME
      ````

3. Install Additional Libraries:

   You need to have the following packages installed for Splice Machine to run:

   * curl
   * nscd
   * ntp
   * openssh
   * openssh-clients
   * openssh-server
   * patch
   * rlwrap
   * wget

   You can use the `apt-get` package manager to install each of these packages, using a command line like this:

   ````
   $ sudo apt-get install <packagename>
   ````

4. Configure Additional Parameters:

   You need to complete these steps:

   |Step|Command|
   |:---|:------|
   |Create symbolic link for YARN |`sudo ln -s /usr/bin/java /bin/java`
   |Configure swappiness|`$ echo 'vm.swappiness = 0' &gt;&gt; /etc/sysctl.conf`
   |Create an alias|`$ rm /bin/sh ; ln -sf /bin/bash /bin/sh`

5. Ensure all necessary services are started:

   You can start all necessary services as follows:

   ````
   $ sudo service nscd start  && service ntp start  && service ssh start
   ````

   Finally, you can check that the services are running with the following command:

   ````
   $ service <service_name> status
   ````

### Configure CentOS/Red Hat Enterprise Linux (RHEL) for Splice Machine

Follow these steps to prepare your RHEL computer to work with Splice Machine:

  *NOTE:* Installing on Linux can be a bit tricky, so please use the `pwd` command to ensure that you are in the directory in which you need to be.

1. Install the Java SE Development Kit

   Fire up the `Terminal` app and enter the following commands:

   To install JDK 8, run one of the following sets of commands:

   ````
   $ cd /opt/$ wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u121-b13/e9e7ea248e2c4826b92b3f075a80e441/jdk-8u121-linux-x64.tar.gz"$ tar xzf jdk-8u121-linux-i586.tar.gz
   ````

   or:

   ````
   $ wget http://download.oracle.com/otn-pub/java/jdk/8u121-b13/e9e7ea248e2c4826b92b3f075a80e441/jdk-8u121-linux-x64.rpm$ sudo yum localinstall jdk-8u121-linux-x64.rpm
   ````

   or:

   ````
   $ sudo rpm -ivh jdk-8u121-linux-x64.rpm
   ````

2. Set up `sudo` rights

   Your User ID must be in the `sudoers` file. To do so, please see the following web page:

   ````
   https://www.digitalocean.com/community/tutorials/how-to-edit-the-sudoers-file-on-ubuntu-and-centos
   ````

3. Check that your `$JAVA_HOME` environment variable is set:

   a. Find the path to the java installation on your computer:

      ````
      $ find / -name java
      ````

   b. Configure your `$JAVA_HOME` environment variable to the path you found in the previous step, as follows:

      ````
      $ echo export JAVA_HOME=/opt/jdk1.8.0_121 >/etc/profile.d/javaenv.sh
      ````

   c. Make sure your `$JRE_HOME` variable is set up:

      ````
      $ echo export JRE_HOME=/opt/jdk1.8.0_121/jre >/etc/profile.d/javaenv.sh
      ````

   d. Set up your `$PATH` variable for your JVM:

      ````
      $ echo export PATH=$PATH:/opt/jdk1.8.0_121/bin:/opt/jdk1.8.0_121/jre/bin >/etc/profile.d/javaenv.sh
      ````

   e. Confirm the variables are set with the echo command (you may need to reload your terminal first):

      ````
      $ echo $JAVA_HOME
      ````

4. Install Additional Libraries:

   You need to have the following packages installed for Splice Machine to run:

   curl, nscd, ntp, openssh, openssh-clients, openssh-server, patch,
   rlwrap, wget, ftp, nc, EPEL repository

   * curl
   * EPEL repository
   * ftp
   * nc
   * nscd
   * ntp
   * openssh
   * openssh-clients
   * openssh-server
   * patch
   * rlwrap
   * wget

   You can install these libraries as follows:

   a. To update CentOS:

      ````
      $ yum update
      ````

   b. To install a binary:

      ````
      $ yum install <packagename>
      ````

   c. To install EPEL repository:

      ````
      $ yum -y install epel-release
      ````

   d. To test if a package is installed:

      ````
      $ yum info <packagename>
      ````

5. Configure Additional Parameters:

   Execute this command:

   ````
   $ sed -i '/requiretty/ s/^/#/' /etc/sudoers
   ````

6. Ensure all necessary services are started:

   You can start all necessary services as follows:

   ````
   $ /sbin/service nscd start  && /sbin/service ntpd start  && /sbin/service sshd start
   ````

   Finally, you can check that the services are running with the following command:

   ````
   $ chkconfig --list
   ````

## Install Splice Machine

Now that you've got your system configured for Splice Machine, let's
download and install the standalone version of Splice Machine, and start
using it!

1. Download the Splice Machine installer.

   Visit this page: <a href="https://www.splicemachine.com/get-started/download/">https://www.splicemachine.com/get-started/download/</a>

2. Copy the downloaded tarball (.gz file) to the directory on your computer in which you want to install Splice Machine

   You should only install in a directory whose name does not contain spaces, because some scripts will not operate correctly if the working directory has spaces in its name.

3. Install Splice Machine:

   Unpack the tarball `gz` file that you downloaded: <a href="https://s3.amazonaws.com/splice-releases/2.5.0.1802/standalone/splicemachine-2.5.0.1729.tar.gz">https://s3.amazonaws.com/splice-releases/2.5.0.1802/standalone/splicemachine-2.5.0.1729.tar.gz</a>

   This creates a `splicemachine` subdirectory and installs Splice Machine software in it.

## Start Using Splice Machine

Start Splice Machine on your computer and run a few commands to verifythe installation:

1. Make your install directory the current directory

   ````
   cd splicemachine
   ````

2. Run the Splice Machine start-up script

   ````
   ./start-splice.sh
   ````

   Initialization of the database may take a couple minutes. It is ready for use when you see this message:

   ````
   Splice Server is ready
   ````

3.  Start using the Splice Machine command line interpreter by launching the `sqlshell.sh` script:

   ````
   ./sqlshell.sh
   ````

   Once you have launched the command line interpreter (the splice&gt; prompt), we recommend verifying that all is well by running a few sample commands. First:

   ````
   splice> show tables;
   ````

   You'll see the names of the tables that are already defined in the
   Splice Machine database; namely, the system tables. Once that works,
   you know Splice Machine is alive and well on your computer, and you
   can use help to list the available commands:

   ````
   splice> help;
   ````

   When you're ready to exit Splice Machine:

   ````
   splice> exit;
   ````

   *NOTE:*  Make sure you end each command with a semicolon (`;`), followed by the *Enter* key or *Return* key.
