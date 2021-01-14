#!/bin/bash
DIR=$(pwd)

if hash rlwrap 2>/dev/null; then
    echo -en "\n ========= rlwrap detected and enabled.  Use up and down arrow keys to scroll through command line history. ======== \n\n"
    RLWRAP=rlwrap
else
    echo -en "\n ========= rlwrap not detected.  Consider installing for command line history capabilities. ========= \n\n"
    RLWRAP=
fi

echo "Running Splice Machine SQL shell"
echo "For help: \"splice> help;\""


if [ $# -ge 1 ] && [ $1 = "debug" ]
then
    # debug: use like
    # ./sqlshell.sh debug # debug port = 4010 (default)
    # or
    # /sqlshell.sh debug 5005 # debug port = 5005
    port=4010
    if [ $# -eq 2 ]
    then
        port=$2
    fi
    export MAVEN_OPTS="${MAVEN_OPTS} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$port"
fi


if [ -z "${CLIENT_SSL_KEYSTORE}" ]; then
  cd splice_machine ; ${RLWRAP} mvn exec:java ; cd ${DIR}
else
  cd splice_machine ; ${RLWRAP} mvn exec:java \
    -Djavax.net.ssl.keyStore=${CLIENT_SSL_KEYSTORE} \
    -Djavax.net.ssl.keyStorePassword=${CLIENT_SSL_KEYSTOREPASSWD} \
    -Djavax.net.ssl.trustStore=${CLIENT_SSL_TRUSTSTORE} \
    -Djavax.net.ssl.trustStore.ssl.trustStorePassword=${CLIENT_SSL_TRUSTSTOREPASSWD} \
    ; cd ${DIR}
fi

