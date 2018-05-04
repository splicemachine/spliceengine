#!/bin/bash

# Defaults
URL=""
HOST="localhost"
PORT="1527"
USER="splice"
PASS="admin"
PROMPT=0
SECURE=0
PRINCIPAL=""
KEYTAB=""
declare -i WIDTH=128
SCRIPT=""
OUTPUT=""
QUIET=0

# Splice Machine SQL Shell

message() {
   local msg="$*"

   if (( ! $QUIET )); then
      echo -e $msg
   fi
}

show_help() {
   echo "Splice Machine SQL client wrapper script"
   echo "Usage: $(basename $BASH_SOURCE) [-U url|-h host] [-p port] [-u user] [-s pass] [-P] [-S] [-k principal] [-K keytab] [-w width] [-f scriptfile] [-o output] [-q]"
   echo -e "\t-U url\t\t full JDBC URL for Splice Machine database"
   echo -e "\t-h host\t\t IP address or hostname of Splice Machine (HBase RegionServer)"
   echo -e "\t-p port\t\t Port which Splice Machine is listening on, defaults to 1527"
   echo -e "\t-u user\t\t username for Splice Machine database"
   echo -e "\t-s pass\t\t password for Splice Machine database"
   echo -e "\t-P \t\t prompt for unseen password"
   echo -e "\t-S \t\t use ssl=basic on connection"
   echo -e "\t-k principal\t kerberos principal (for kerberos)"
   echo -e "\t-K keytab\t kerberos keytab - requires principal"
   echo -e "\t-w width \t output row width. defaults to 128"
   echo -e "\t-f script\t sql file to be executed"
   echo -e "\t-o output\t file for output"
   echo -e "\t-q \t\t quiet mode"
}

HOSTARG=""
PORTARG=""
USERARG=""
PASSARG=""

# Process command line args
while getopts "U:h:p:u:s:PSk:K:w:f:o:q" opt; do
   case $opt in
      U)
         URL="${OPTARG}"
         ;;
      h)
         HOSTARG="${OPTARG}"
         ;;
      p)
         PORTARG="${OPTARG}"
         ;;
      u)
         USERARG="${OPTARG}"
         ;;
      s)
         PASSARG="${OPTARG}"
         ;;
      P)
         PROMPT=1
         ;;
      S)
         SECURE=1
         ;;
      k)
         PRINCIPAL="${OPTARG}"
         ;;
      K)
         KEYTAB="${OPTARG}"
         ;;
      w)
         WIDTH="${OPTARG}"
         ;;
      f)
         SCRIPT="${OPTARG}"
         ;;
      o)
         OUTPUT="${OPTARG}"
         ;;
      q)
         QUIET=1
         ;;
      \?)
         show_help
         exit 1
         ;;
   esac
done

# Validate options
if [[ "$URL" != "" ]]; then
   if [[ "$HOSTARG" != "" ]]; then
      echo "Error: you cannot supply both a URL and the -h host option"
      exit 1
   elif [[ "$PORTARG" != "" ]]; then
      echo "Error: you cannot supply both a URL and the -p port option"
      exit 1
   elif [[ "$USERARG" != "" ]]; then
      echo "Error: you cannot supply both a URL and the -u user option"
      exit 1
   elif [[ "$PASSARG" != "" ]]; then
      echo "Error: you cannot supply both a URL and the -s password option"
      exit 1
   elif (( $SECURE )); then
      echo "Error: you cannot supply both a URL and the -S ssl flag
      exit 1
   elif [[ "$PRINCIPAL" != "" ]]; then
      echo "Error: you cannot supply both a URL and the -k principal option"
      exit 1
   elif [[ "$KEYTAB" != "" ]]; then
      echo "Error: you cannot supply both a URL and the -K keytab option"
      exit 1
   fi
fi

# check for jdk1.8 and exit if not found
if ( type -p java >/dev/null); then
   _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
   _java="$JAVA_HOME/bin/java"
else
   echo "Error: no java found. $0 requires java."
   show_help
   exit 1
fi

jversion=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
message "java version ${jversion}\n"
if [[ "$jversion" < "1.8" ]]; then
   echo "Error: java is older than 1.8.  $0 requires java 1.8"
   show_help
   exit 1
fi

# set splice lib dir
SPLICE_LIB_DIR="##SPLICELIBDIR##"

if [[ "$SPLICE_LIB_DIR" == *"##"* ]]; then
   CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

   # ends in bin, go up and over to lib
   if [[ "$CURDIR" = *"bin" ]]; then
      SPLICE_LIB_DIR="${CURDIR}/../lib"
   else #subdir
      SPLICE_LIB_DIR="${CURDIR}/lib"
   fi
fi

# set up classpath to point to splice jars
export CLASSPATH="${SPLICE_LIB_DIR}/*"

if [[ "$HOSTARG" != "" ]]; then
   HOST=$HOSTARG
fi
if [[ "$PORTARG" != "" ]]; then
   PORT=$PORTARG
fi
if [[ "$USERARG" != "" ]]; then
   USER=$USERARG
fi

# prompt silently for user password
if (( $PROMPT )); then
   read -s -p "Enter Password: " PASS
elif [[ "${PASSARG}" != "" ]];
   PASS=$PASSARG
fi

GEN_SYS_ARGS="-Djava.awt.headless=true"

# Setup IJ_SYS_ARGS based on input options
IJ_SYS_ARGS="-Djdbc.drivers=com.splicemachine.db.jdbc.ClientDriver"

# add width via ij.maximumDisplayWidth
if [[ "$WIDTH" != "128" ]]; then
   IJ_SYS_ARGS+=" -Dij.maximumDisplayWidth=${WIDTH}"
fi

if [[ "$OUTPUT" != "" ]]; then
   # figure out if OUTPUT directory exists
   outpath=$(dirname $OUTPUT)
   if [[ ! -d $outpath ]]; then
      echo Error: you specified a non-existant directory for output $OUTPUT
      exit 2
   fi
   IJ_SYS_ARGS+=" -Dij.outfile=${OUTPUT}"
fi

if [[ "$URL" != "" ]]; then
   IJ_SYS_ARGS+=" -Dij.connection.splice=${URL}"
else
   # Add optional URL parameters
   SSL=""
   KERBEROS=""
   if (( $SECURE )); then
      SSL=";ssl=basic"
   fi
   if [[ ${PRINCIPAL} != "" ]]; then
      KERBEROS=";principal=${PRINCIPAL}"
      if [ -n "$KEYTAB" ] ; then
         KERBEROS+=";keytab=${KEYTAB}"
      fi
   fi
   IJ_SYS_ARGS+=" -Dij.connection.splice=jdbc:splice://${HOST}:${PORT}/splicedb;user=${USER};password=${PASS}${SSL}${KERBEROS}"
fi

if hash rlwrap 2>/dev/null; then
   message "\n ========= rlwrap detected and enabled.  Use up and down arrow keys to scroll through command line history. ======== \n"
   RLWRAP=rlwrap
else
   message "\n ========= rlwrap not detected.  Consider installing for command line history capabilities. ========= \n"
   RLWRAP=
fi

message "Running Splice Machine SQL shell"
message "For help: \"splice> help;\""
${RLWRAP} java ${GEN_SYS_ARGS} ${IJ_SYS_ARGS}  com.splicemachine.db.tools.ij ${SCRIPT}
