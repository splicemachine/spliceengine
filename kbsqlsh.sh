#!/bin/bash

# This version supports both username/password -u/-s and principal/keytab -P/-K
# If -C or -P is not specified, then authentication using user/password (-u/-s)is assumed
# This version does not support the SSL encryption which was added in a later version of sqlshell.sh

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
HOST="localhost"
PORT="1527"
SPLICE_USER=""
SPLICE_PASS=""
PRINCIPAL=""
KEYTAB=""
CACHED=""
SCRIPT=""

VERBOSE=0

# Splice Machine SQL Shell

show_help()
{
        echo ""
        echo "Kerberized Splice Machine SQL client wrapper script"
        echo "Usage: $(basename $BASH_SOURCE) [-h host] [-p port ] [[-u username][-s password] | -C | [-P principal][-K keytab]] [-f scriptfile]"
        echo -e "\t-h IP addreess or hostname of Splice Machine (HBase RegionServer)"
        echo -e "\t-p Port which Splice Machine is listening on, defaults to 1527"
        echo -e "\t-u username for Splice Machine database"
        echo -e "\t-s password for Splice Machine database"
        echo -e "\t-P principal name of user of Splice Machine database"
        echo -e "\t-K keytab file for principal of Splice Machine database"
        echo -e "\t-C use the cached principal name as found by kinit command" 
        echo -e "\t-f sql file to be executed"
        echo -e "\t-v verbose"
        echo -e "\tOne of the options must be set: -u, -C or -P."
        echo -e "\tKerberos principal -C/-P takes precedence over user/password"

        echo -e "\tThree different ways of using Principal/Kerberos:"
        echo -e "\t1. Specify the -C cached only. This shell would get the cached principal from klist."
        echo -e "\t2. Specify the -P principal only. A valid kinit session with a non-expired TGT is assumed for this case and case 1."
        echo -e "\t3. Specify the -P principal and -K keytab file."
}

fatal_error() 
{
  local MSG="$1"
  echo "ERROR: $MSG"
  show_help
  exit 1
}

check_arg()
{
  if [ -n "$SPLICE_USER" ] ; then
    return
  fi

  # Check validity of PRINCIPAL and KEYTAB if set
  if  [ -n "$KEYTAB" ] && [ ! -r "$KEYTAB" ] ; then
      fatal_error "Keytab file $KEYTAB is specified but is not readable or cannot be found"
  fi

  if [ -z "$CACHED" ] &&  [ -z "$PRINCIPAL"  ] ; then
       fatal_error "Either -C or -P must be specfied"
  fi
  
  # -C set: either use PRINCIPAL provided, or the one found in cache
  if [ -n "$CACHED" ] ; then
    if [ -n "$PRINCIPAL" ] ; then	
      return
    fi
    # PRINCIPAL not specified. Now find it in the credential cache by klist
    PRINC=$(  klist | grep principal: | awk -F: '{print $2}'  | sed -e 's/ //g' )
    if [ -z "$PRINC" ] ; then
      fatal_error "Failed to find the pricipal from the credentials cache"
    fi
    PRINCIPAL="$PRINC"
  fi
}

echo_arg()
{
  if [ "$VERBOSE" == 0 ] ; then
    return
  fi
  echo "VERBOSE: " $*
}

get_password()
{
  if [ -n "$PRINCIPAL" ]; then
    return
  fi
  if [ -n "$SPLICE_PASS" ]; then
    return
  fi
  read -s -p "Enter Password: " SPLICE_PASS
}


# Process command line args
while getopts "h:p:f:u:s:P:K:Cv" opt; do
    case $opt in
        h)
                HOST="${OPTARG}"
                ;;
        p)
                PORT="${OPTARG}"
                ;;
        f)
                SCRIPT="${OPTARG}"
                ;;
        u)
                SPLICE_USER="${OPTARG}"
                ;;
        s)
                SPLICE_PASS="${OPTARG}"
                ;;
        P)
                PRINCIPAL="${OPTARG}"
                ;;
        K)
                KEYTAB="${OPTARG}"
                ;;
        C)
                CACHED=1
                ;;
        v)
                VERBOSE=1
                ;;
        \?)
                show_help
                exit 1
                ;;
    esac
done

check_arg
get_password

# set hbase lib dir here to keep it in one place.
SPLICE_LIB_DIR="/opt/cloudera/parcels/SPLICEMACHINE/lib"

# set up classpath to point to splice jars
export CLASSPATH="${SPLICE_LIB_DIR}/*"

GEN_SYS_ARGS="-Djava.awt.headless=true"

# original user/password, then the kerberized version: with principal only, and with both principal and keytab
IJ_SYS_ARGS="-Djdbc.drivers=com.splicemachine.db.jdbc.ClientDriver -Dij.connection.splice=jdbc:splice://${HOST}:${PORT}/splicedb;user=${SPLICE_USER};password=${SPLICE_PASS}"

IJ_KBR_PRINCIPAL_ARGS="-Djdbc.drivers=com.splicemachine.db.jdbc.ClientDriver -Dij.connection.splice=jdbc:splice://${HOST}:${PORT}/splicedb;principal=${PRINCIPAL}"

IJ_KBR_FULL_ARGS="-Djdbc.drivers=com.splicemachine.db.jdbc.ClientDriver -Dij.connection.splice=jdbc:splice://${HOST}:${PORT}/splicedb;principal=${PRINCIPAL};keytab=${KEYTAB}"



if hash rlwrap 2>/dev/null; then
    echo -en "\n ========= rlwrap detected and enabled.  Use up and down arrow keys to scroll through command line history. ======== \n\n"
    RLWRAP=rlwrap
else
    echo -en "\n ========= rlwrap not detected.  Consider installing for command line history capabilities. ========= \n\n"
    RLWRAP=
fi

if [ -n "$PRINCIPAL" ]; then
  echo "Running Splice Machine SQL shell in kerberos mode:"
  echo "For help: \"splice> help;\""
  if [ -n "$KEYTAB" ] ; then
     
     echo_arg ${RLWRAP} java ${GEN_SYS_ARGS} ${IJ_KBR_FULL_ARGS}  com.splicemachine.db.tools.ij ${SCRIPT}
     ${RLWRAP} java ${GEN_SYS_ARGS} ${IJ_KBR_FULL_ARGS}  com.splicemachine.db.tools.ij ${SCRIPT}
  else
     echo_arg {RLWRAP} java ${GEN_SYS_ARGS} ${IJ_KBR_PRINCIPAL_ARGS}  com.splicemachine.db.tools.ij ${SCRIPT}
     ${RLWRAP} java ${GEN_SYS_ARGS} ${IJ_KBR_PRINCIPAL_ARGS}  com.splicemachine.db.tools.ij ${SCRIPT}
  fi
else
  echo "Running Splice Machine SQL shell"
  echo "For help: \"splice> help;\""
  echo_arg ${RLWRAP} java ${GEN_SYS_ARGS} ${IJ_SYS_ARGS}  com.splicemachine.db.tools.ij ${SCRIPT}
  ${RLWRAP} java ${GEN_SYS_ARGS} ${IJ_SYS_ARGS}  com.splicemachine.db.tools.ij ${SCRIPT}
fi

stat=$?
exit $stat

