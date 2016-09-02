#!/bin/bash

UNAME=$(uname -s)

_retrySplice() {
    ROOT_DIR="${1}"
    SPLICELOGFILE="${2}"
    ZOOLOGFILE="${3}"
    LOG4J_PATH="${4}"
    ZOO_DIR="${5}"
    ZOO_WAIT_TIME="${6}"
    HBASE_ROOT_DIR_URI="${7}"
    CP="${8}"
    SPLICE_MAIN_CLASS="${9}"
    CHAOS="${10}"
    SPARK="${11}"

    # number of seconds we should allow for isReady to return 0
    HBASE_TIMEOUT=100
    # Zookeeper takes very little time
    ZOO_TIMEOUT=15
    if [[ ${UNAME} == CYGWIN* ]]; then
        # be a more lenient with Cygwin
        HBASE_TIMEOUT=200
        ZOO_TIMEOUT=30
    fi

    echo "Starting Splice Machine..."
    echo "Log file is ${SPLICELOGFILE}"
    echo "Waiting for Splice..."

    MAXRETRY=3

    # Start Zookeeper
    RETURN_CODE=0
    ERROR_CODE=0
    for (( RETRY=1; RETRY<=MAXRETRY; RETRY++ )); do
        _startZoo "${ROOT_DIR}" "${ZOOLOGFILE}" "${LOG4J_PATH}" "${ZOO_DIR}" "${CP}"
        _waitfor "${ZOOLOGFILE}" "${ZOO_TIMEOUT}" 'Snapshotting'
        RETURN_CODE=$?
        if [[ ${RETURN_CODE} -eq 0 ]]; then
            ERROR_CODE=0
            break
        else
            if [[ ${RETRY} -lt ${MAXRETRY} ]]; then
                if [[ -e "${ROOT_DIR}"/zoo_pid ]]; then
                    _stop "${ROOT_DIR}"/zoo_pid 45 
                fi
                ERROR_CODE=1
            else
                ERROR_CODE=1
            fi
        fi
    done

    if [ "${SPARK}" = true ]; then
        # Start Spark
        _startSpark "${ROOT_DIR}" "${CP}" "${SPLICELOGFILE}"
    fi

    # Start HBase
    if [[ ${ERROR_CODE} -eq 0 ]]; then
        for (( RETRY=1; RETRY<=MAXRETRY; RETRY++ )); do
            # splice/hbase will be retried several times to accommodate timeouts
            _startSplice "${ROOT_DIR}" "${SPLICELOGFILE}" "${LOG4J_PATH}" "${HBASE_ROOT_DIR_URI}" "${CP}" "${SPLICE_MAIN_CLASS}" "${CHAOS}" "${SPARK}"
            if [[ ${RETRY} -eq 1 ]]; then
                # We can only check for error msg the first time, else we'll see the same ones again
                _waitfor "${SPLICELOGFILE}" "${HBASE_TIMEOUT}" 'Ready to accept JDBC connections' 'Master not active after'
            else
                _waitfor "${SPLICELOGFILE}" "${HBASE_TIMEOUT}" 'Ready to accept JDBC connections'
            fi
            RETURN_CODE=$?
            if [[ ${RETURN_CODE} -eq 0 ]]; then
                ERROR_CODE=0
                break
            else
                if [[ ${RETRY} -lt ${MAXRETRY} ]]; then
                    if [[ -e "${ROOT_DIR}"/splice_pid ]]; then
                        _stop "${ROOT_DIR}"/splice_pid 65
                    fi
                    ERROR_CODE=1
                else
                    ERROR_CODE=1
                fi
            fi
        done
    fi

    if [[ ${ERROR_CODE} -ne 0 ]]; then
        if [[ -e "${ROOT_DIR}"/splice_pid || -e "${ROOT_DIR}"/zoo_pid ]]; then
            "${ROOT_DIR}"/bin/stop-splice.sh
        fi
        echo
        echo "Splice didn't start as expected. Please restart with the \"-d\" option and check ${SPLICELOGFILE}." >&2
        return 1;
    else
        echo
        echo "Splice Server is ready"
        echo "The HBase URI is http://localhost:60010"
        echo "The JDBC URI is jdbc:splice://localhost:1527/splicedb"
        return 0;
    fi
}

_startSpark() {
    ROOT_DIR="${1}"
    CLASSPATH="${2}"
    LOGFILE="${3}"

    # setup compute-classpath.sh
    mkdir -p "${ROOT_DIR}"/spark/bin
    echo "echo ${CLASSPATH}" > "${ROOT_DIR}"/spark/bin/compute-classpath.sh
    chmod +x "${ROOT_DIR}"/spark/bin/compute-classpath.sh

    export CLASSPATH
    (java org.apache.spark.deploy.master.Master -i `hostname` >> "${LOGFILE}" 2>&1 ) &
    echo "$!" > "${ROOT_DIR}"/spark_master_pid
    (java org.apache.spark.deploy.worker.Worker spark://`hostname`:7077 -d "${ROOT_DIR}"/spark >> "${LOGFILE}" 2>&1 ) &
    echo "$!" > "${ROOT_DIR}"/spark_worker_pid
}

_startSplice() {
    ROOT_DIR="${1}"
    LOGFILE="${2}"
    LOG4J_PATH="${3}"
    HBASE_ROOT_DIR_URI="${4}"
    CLASSPATH="${5}"
    SPLICE_MAIN_CLASS="${6}"
    CHAOS="${7}"
    SPARK="${8}"

    SPLICE_PID_FILE="${ROOT_DIR}"/splice_pid
    export CLASSPATH
    LOG4J_CONFIG="-Dlog4j.configuration=$LOG4J_PATH"

    # Spark config
    SPARK_MASTER_HOST=`hostname`
    if [ "${SPARK}" = true ]; then
        SPARK_CONFIG="-Dsplice.spark.enabled=true -Dsplice.spark.master=spark://${SPARK_MASTER_HOST}:7077 -Dsplice.spark.home=${ROOT_DIR}/spark "
    else
        SPARK_CONFIG=""
    fi


    SYS_ARGS="-verbose:gc \
    -Xdebug \
    ${LOG4J_CONFIG} \
    ${SPARK_CONFIG} \
    -Djava.net.preferIPv4Stack=true \
    -Djava.awt.headless=true \
    -Dzookeeper.sasl.client=false \
    -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.port=10102 \
    -Dsplice.authentication=NATIVE \
    -Xmx8g \
    -Xms3g \
    -Xmn128m \
    -XX:+UseConcMarkSweepGC \
    -XX:+UseParNewGC \
    -XX:NewSize=1024m \
    -XX:MaxNewSize=1024m \
    -XX:CMSInitiatingOccupancyFraction=70 \
    -XX:+UseCMSInitiatingOccupancyOnly \
    -XX:MaxGCPauseMillis=100 \
    -XX:+CMSClassUnloadingEnabled \
    -XX:MaxDirectMemorySize=1g \
    -Dderby.language.updateSystemProcs=false \
    -Dsplice.debug.logStatementContext=false"
	# -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=4000

    if [[ -n ${SPLICE_SYS_ARGS} ]]; then
        SYS_ARGS="${SYS_ARGS} ${SPLICE_SYS_ARGS}"
    fi
        #HBase port properties
    HBASE_MASTER_PORT=60000
    HBASE_MASTER_INFO_PORT=60010
    HBASE_REGIONSERVER_PORT=60020
    HBASE_REGIONSERVER_INFO_PORT=60030
    SPLICE_PORT=1527

    (java ${SYS_ARGS} "${SPLICE_MAIN_CLASS}" "${HBASE_ROOT_DIR_URI}" ${HBASE_MASTER_PORT} ${HBASE_MASTER_INFO_PORT} ${HBASE_REGIONSERVER_PORT} ${HBASE_REGIONSERVER_INFO_PORT} ${SPLICE_PORT} ${CHAOS} >> "${LOGFILE}" 2>&1 ) &
    echo "$!" > ${SPLICE_PID_FILE}
}

_startZoo() {
    ROOT_DIR="${1}"
    LOGFILE="${2}"
    LOG4J_PATH="${3}"
    ZOO_DIR="${4}"
    CLASSPATH="${5}"

    ZOO_PID_FILE="${ROOT_DIR}"/zoo_pid
    export CLASSPATH
    LOG4J_CONFIG="-Dlog4j.configuration=$LOG4J_PATH"

    SYS_ARGS=" -Xmx2g -Xms1g -Dzookeeper.sasl.client=false -Djava.awt.headless=true ${LOG4J_CONFIG} -Djava.net.preferIPv4Stack=true"

    (java ${SYS_ARGS} org.apache.zookeeper.server.ZooKeeperServerMain 2181 "${ZOO_DIR}" 10 0  > "${LOGFILE}" 2>&1 ) &
    echo "$!" > ${ZOO_PID_FILE}
}

_stop() {
    PID_FILE="${1}"
    TIMEOUT="${2}"

    if [[ ! -e "${PID_FILE}" ]]; then
        echo "${PID_FILE} is not running."
        #echo "Double check and kill any straggler"
        if [[ "$PID_FILE" = *"splice_pid"* ]]; then
            S=$(ps -ef | awk '/SpliceTestPlatform/ && !/awk/ {print $2}')
        fi
        if [[ "$PID_FILE" = *"zoo_pid"* ]]; then
            S=$(ps -ef | awk '/ZooKeeperServerMain/ && !/awk/ {print $2}')
        fi
        if [[ -n ${S} ]]; then
            for pid in ${S}; do
                kill -15 ${pid}
            done
        fi
        return 0;
    fi

    KILL_PID=$(grep -oE "[0-9]+" ${PID_FILE}) # Make sure we get only digits

    if [[ -n "${KILL_PID}" ]]; then

        ALIVE_PID=$(ps -p ${KILL_PID} | awk ' !/PID/ { print $1 }')
        if [[ -z "${ALIVE_PID}" || "${ALIVE_PID}" -ne "${KILL_PID}" ]]; then
            #echo "No process PID [${KILL_PID}] dead?"
            # clean up old pid file
            if [[ ${UNAME} == CYGWIN* ]]; then
				/bin/rm -f "$(cygpath --path --unix ${PID_FILE})"
            else
                /bin/rm -f "${PID_FILE}"
            fi
            return 1;
        fi

        if [[ -n "$KILL_PID" ]]; then
            kill -15 ${KILL_PID}
            _sleep ${TIMEOUT}
        fi

        ALIVE_PID=$(ps -p ${KILL_PID} | awk ' !/PID/ { print $1 }')
        if [[ -n "$ALIVE_PID" ]]; then
            echo "Process didn't shut down. Trying again..."
            kill -9 ${KILL_PID}
        fi
    else
        echo "Bad PID: [${KILL_PID}] in ${PID_FILE}"
        # clean up old pid file
        if [[ ${UNAME} == CYGWIN* ]]; then
			/bin/rm -f "$(cygpath --path --unix ${PID_FILE})"
        else
            /bin/rm -f "${PID_FILE}"
        fi
        return 1;
    fi

    # clean up old pid file
    if [[ ${UNAME} == CYGWIN* ]]; then
		/bin/rm -f "$(cygpath --path --unix ${PID_FILE})"
    else
        /bin/rm -f "${PID_FILE}"
    fi
}

_stopServer() {
    ROOT_DIR="${1}"
    PID_DIR="${2}"

    echo "Shutting down Splice..."
    # shut down splice/hbase, timeout value is hardcoded
    _stop "${PID_DIR}"/splice_pid 65

    echo "Shutting down Zookeeper..."
    # shut down zookeeper, timeout value is hardcoded
    _stop "${PID_DIR}"/zoo_pid 15

    echo "Shutting down Spark worker..."
    # shut down spark worker, timeout value is hardcoded
    _stop "${PID_DIR}"/spark_worker_pid 15

    echo "Shutting down Spark master..."
    # shut down spark worker, timeout value is hardcoded
    _stop "${PID_DIR}"/spark_master_pid 15
}

_waitfor() {
    LOGFILE="${1}"
    TIMEOUT="${2}"
    LOG_SUCCESS="${3}"
    LOG_FAILED="${4}"
    if [[ ${TIMEOUT} -le 0 ]]; then
        echo "TIMEOUT value must be positive"
        return 1;
    fi

    # fix for DB-1643
    if [ "$LOG_SUCCESS" = "Snapshotting" ]; then
        # number of seconds we should wait between checks on clean status
        INTERVAL=2
        # total number of seconds we should wait for clean status
        (( t = TIMEOUT ))
        while (( t > 0 )); do
            if [[ ${UNAME} == CYGWIN* ]]; then
                S=$(grep Snapshotting $LOGFILE)
                if [ -n "${S}" ]; then
                    echo "Detected zookeeper running"
                    return 0;
                fi
            else
                S=$(echo stat | nc localhost 2181 | grep Mode:)
                if [ -n "${S}" ]; then
                    echo "Detected zookeeper running@localhost in $S"
                    return 0;
                fi
            fi
            # Show something while we wait
            echo -ne "."
            sleep ${INTERVAL}

            # Still coming up... continue;
            (( t -= INTERVAL ))
        done
        # did not start in allotted timeout - error
        return 1;
    fi
    # fix for DB-1643 

    # number of seconds we should wait between checks on clean status
    INTERVAL=2
    # total number of seconds we should wait for clean status
    (( t = TIMEOUT ))
    while (( t > 0 )); do
        # Poll the log to check for success msg
        OLDIFS=$IFS
        IFS=$'\n'
        if [[ -n ${LOG_FAILED} ]]; then
            # Failed
			FAILED=$(grep "${LOG_FAILED}" "${LOGFILE}")
            if [ -n "${FAILED}" ]; then
                # Error in log - fail
                IFS=${OLDIFS}
                return 1;
            fi
        fi

        # Ready
		SUCCESS=$(grep "${LOG_SUCCESS}" "${LOGFILE}")
        IFS=${OLDIFS}
        if [ -n "${SUCCESS}" ]; then
            # Started, ready - success
            return 0;
        fi

        # Show something while we wait
        echo -ne "."
        sleep ${INTERVAL}

        # Still coming up... continue;
        (( t -= INTERVAL ))
    done
    # did not start in allotted timeout - error
    return 1;
}

_sleep() {
    TIMEOUT="${1}"

    NUM_INTERVALS=10
    # Number of seconds we should wait between printing status.
    INTERVAL=$(( TIMEOUT / NUM_INTERVALS ))
    if [[ ${INTERVAL} -eq 0 ]]; then
		INTERVAL=1
    fi
    # Total number of seconds we should wait.
    (( t = TIMEOUT ))
    while (( t > 0 )); do
        # Show something while we wait.
        echo -ne "\r$(( ( ( TIMEOUT - t ) * 100 / TIMEOUT ) ))%"
                sleep ${INTERVAL}
        # Time left...
        (( t -= INTERVAL ))
    done
    echo -e "\r100%"
}
