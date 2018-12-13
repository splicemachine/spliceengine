#!/bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
HOST="localhost"
PORT="1527"
USER="splice"
PASS="admin"
SCRIPT=""

# Splice Machine SQL Shell

show_help()
{
	echo "Splice Machine SQL client wrapper script"
	echo "Usage: $(basename $BASH_SOURCE) [-h host] [-p port ] [-u username] [-s password] [-f scriptfile]"
	echo -e "\t-h IP addreess or hostname of Splice Machine (HBase RegionServer)"
	echo -e "\t-p Port which Splice Machine is listening on, defaults to 1527"
	echo -e "\t-u username for Splice Machine database"
	echo -e "\t-s password for Splice Machine database"
	echo -e "\t-f sql file to be executed"
}

# Process command line args
while getopts "h:p:u:s:f:" opt; do
    case $opt in
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
	f)
		SCRIPT="${OPTARG}"
		;;
	\?)
		show_help
		exit 1
		;;
    esac
done

# set hbase lib dir here to keep it in one place.
SPLICE_LIB_DIR="/opt/splice/default/lib"

# set up classpath to point to system HBase jars
export CLASSPATH="${SPLICE_LIB_DIR}/*"

GEN_SYS_ARGS="-Djava.awt.headless=true"

IJ_SYS_ARGS="-Djdbc.drivers=com.splicemachine.db.jdbc.ClientDriver -Dij.connection.splice=jdbc:splice://${HOST}:${PORT}/splicedb;user=${USER};password=${PASS}"

if hash rlwrap 2>/dev/null; then
    echo -en "\n ========= rlwrap detected and enabled.  Use up and down arrow keys to scroll through command line history. ======== \n\n"
    RLWRAP=rlwrap
else
    echo -en "\n ========= rlwrap not detected.  Consider installing for command line history capabilities. ========= \n\n"
    RLWRAP=
fi

echo "Running Splice Machine SQL shell"
echo "For help: \"splice> help;\""
${RLWRAP} java ${GEN_SYS_ARGS} ${IJ_SYS_ARGS}  com.splicemachine.db.tools.ij ${SCRIPT}
