#!/bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
HOST="localhost"
PORT="1527"
USER="splice"
PASS="admin"
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
        echo "Usage: $(basename $BASH_SOURCE) [-U url|-h host] [-p port] [-u username] [-s password] [-w width] [-f scriptfile] [-o output] [-q]"
        echo -e "\t-U full JDBC URL for Splice Machine database"
        echo -e "\t-h IP address or hostname of Splice Machine (HBase RegionServer)"
        echo -e "\t-p Port which Splice Machine is listening on, defaults to 1527"
        echo -e "\t-u username for Splice Machine database"
        echo -e "\t-s password for Splice Machine database"
        echo -e "\t-w width of output lines. defaults to 128"
        echo -e "\t-f sql file to be executed"
        echo -e "\t-o file for output"
        echo -e "\t-q quiet mode"
}

# Process command line args
while getopts "U:h:p:u:s:w:f:o:q" opt; do
    case $opt in
        U)
                URL="${OPTARG}"
                ;;
        h)
                HOST="${OPTARG}"
                ;;
        p)
                PORT="${OPTARG}"
                ;;
        u)
                USER="${OPTARG}"
                ;;
        s)
                PASS="${OPTARG}"
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

# set hbase lib dir here to keep it in one place.
SPLICE_LIB_DIR="##SPLICELIBDIR##"

# set up classpath to point to splice jars
export CLASSPATH="${SPLICE_LIB_DIR}/*"

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
   IJ_SYS_ARGS+=" -Dij.connection.splice=jdbc:splice://${HOST}:${PORT}/splicedb;user=${USER};password=${PASS}"
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
