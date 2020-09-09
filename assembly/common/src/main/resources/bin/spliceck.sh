#!/bin/bash

# set splice lib dir
SPLICE_JARS="##SPLICELIBDIR##"

if [[ "SPLICE_JARS" == *"##"* ]]; then
   CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

   # ends in bin, go up and over to lib
   if [[ "$CURDIR" = *"bin" ]]; then
      SPLICE_JARS="${CURDIR}/../lib"
   else #subdir
      SPLICE_JARS="${CURDIR}/lib"
   fi
fi

#check if all the JARs needed are found
if [ "$(ls -A $SPLICE_JARS 2> /dev/null)" ]; then
  SPLICECK=$(find $SPLICE_JARS -type f -name 'splice_ck*.jar')
  HBASE_JARS=$(hbase classpath 2>/dev/null)
  SPARK_JARS="$(readlink -f $(which spark-shell) | xargs dirname)/../jars/*"
  # set up classpath to point to splice jars
  if [ -z "${SPLICECK}" ]; then
    echo "Error: one or more JARs from SpliceMachine required to run splice-ck cannot be found or created."
    exit 1
  elif [ -z "${HBASE_JARS}" ]; then
    echo "Error: could not find HBase JARs required to run splice-ck."
    exit 1
  elif [ -z "${SPARK_JARS}" ]; then
    echo "Error: could not find Spark JARs required to run splice-ck."
    exit 1
  else
    export CLASSPATH="${SPLICE_JARS}/*:${HBASE_JARS}:${SPARK_JARS}"
  fi
else
  echo "Error: the dependency directory at $SPLICE_JARS cannot be found or created."
  exit 1
fi

# https://stackoverflow.com/a/8723305/337194
C=''
for i in "$@"; do
    i="${i//\\/\\\\}"
    C="$C ${i}"
done
java com.splicemachine.ck.command.RootCommand ${C} 2> >(grep -v "^SLF4J" >&2)
