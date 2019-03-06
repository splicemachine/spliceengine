#!/bin/sh

# Splice Machine-customized /opt/conf/mapr-hbase-config.sh

MAPR_HOME="${MAPR_HOME:-/opt/mapr}"
HBASE_VER=1.1.8-splice
HBASE_HOME="${MAPR_HOME}/hbase/hbase${HBASE_VER}"

EXTRA_JARS="gateway-*.jar"
for jar in ${EXTRA_JARS} ; do
  JARS=`echo $(ls ${MAPR_HOME}/lib/${jar} 2> /dev/null) | sed 's/\s\+/:/g'`
  if [ "${JARS}" != "" ]; then
    HBASE_MAPR_EXTRA_JARS=${HBASE_MAPR_EXTRA_JARS}:${JARS}
  fi
done

# Remove any additional ':' from the tail
HBASE_MAPR_EXTRA_JARS="${HBASE_MAPR_EXTRA_JARS#:}"

export HBASE_MAPR_EXTRA_JARS

# Need to call this to continue the daisy chain of config scripts
if [ -f "${HBASE_HOME}/bin/mapr-config.sh" ] ; then
    . "${HBASE_HOME}/bin/mapr-config.sh"
fi
