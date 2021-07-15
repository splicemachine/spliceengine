#!/bin/bash
set -ex


#The following is written to aid local testing
if [ -z $PARCELS_ROOT ] ; then
    export MYDIR=`dirname "${BASH_SOURCE[0]}"`
    PARCELS_ROOT=`cd $MYDIR/../.. &&  pwd`
fi
PARCEL_DIRNAME=${PARCEL_DIRNAME-SPLICE}

MYLIBDIR=${PARCELS_ROOT}/${PARCEL_DIRNAME}/lib

[ -d $MYLIBDIR ] || {
    echo "Could not find splice parcel lib dir, exiting" >&2
    exit 1
}

PREPENDSTRING="${CONF_DIR}:${MYLIBDIR}/*:${PARCELS_ROOT}/CDH/lib/spark/jars/*"
echo "prepending $PREPENDSTRING to HBASE_CLASSPATH_PREFIX"
if [ -z $HBASE_CLASSPATH_PREFIX ] ; then
    export HBASE_CLASSPATH_PREFIX="${PREPENDSTRING}"
else
    export HBASE_CLASSPATH_PREFIX="${PREPENDSTRING}:${HBASE_CLASSPATH_PREFIX}"
fi

echo "Copying yarn-site.xml to hbase directory"

if [ -r "/etc/hadoop/conf/yarn-site.xml" ] ; then
    cp "/etc/hadoop/conf/yarn-site.xml" "$CONF_DIR"
else
    echo "Could not find yarn-site.xml, make sure to deploy yarn client in UI" >&2
    exit 1
fi

echo "Set HBASE_CLASSPATH_PREFIX to '$HBASE_CLASSPATH_PREFIX'"
echo "splice_env.sh successfully executed at `date`"