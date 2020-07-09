@ECHO OFF
SETLOCAL ENABLEDELAYEDEXPANSION

REM defaults
SET "URL="
SET HOST="localhost"
SET PORT=1527
SET USER="splice"
SET QUOTE=0
SET PASS="admin"
SET SECURE=0
SET "PRINCIPAL="
SET "KEYTAB="
SET PROMPT=0
SET WIDTH=128
SET "SCRIPT="
SET "OUTPUT="
SET QUIET=0
SET "HOSTARG="
SET "PORTARG="
SET "USERARG="
SET "PASSARG="
SET SCRIPT_DIR="%~dp0"
SET SCRIPT_NAME=%~n0%~x0

REM read user arguments
:loop
IF NOT [%1] == [] (
  IF "%1" == "-U" (
    SET URL=%2
    SHIFT & SHIFT
    GOTO :loop
  ) ELSE IF "%1" == "-h" (
    SET HOSTARG=%2
    SHIFT & SHIFT
    GOTO :loop
  ) ELSE IF "%1" == "-p" (
    SET PORTARG=%2
    SHIFT & SHIFT
    GOTO :loop
  ) ELSE IF "%1" == "-u" (
    SET USERARG=%2
    SHIFT & SHIFT
    GOTO :loop
  ) ELSE IF "%1" == "-Q" (
    SET QUOTE=1
    SHIFT
    GOTO :loop
  ) ELSE IF "%1" == "-s" (
    SET PASSARG=%2
    SHIFT & SHIFT
    GOTO :loop
  ) ELSE IF "%1" == "-P" (
    SET PROMPT=1
    SHIFT
    GOTO :loop
  ) ELSE IF "%1" == "-S" (
    SET SECURE=1
    SHIFT
    GOTO :loop
  ) ELSE IF "%1" == "-k" (
    SET PRINCIPAL=%2
    SHIFT & SHIFT
    GOTO :loop
  ) ELSE IF "%1" == "-K" (
    SET KEYTAB=%2
    SHIFT & SHIFT
    GOTO :loop
  ) ELSE IF "%1" == "-w" (
    SET WIDTH=%2
    SHIFT & SHIFT
    GOTO :loop
  ) ELSE IF "%1" == "-f" (
    SET SCRIPT=%2
    SHIFT & SHIFT
    GOTO :loop
  ) ELSE IF "%1" == "-o" (
    SET OUTPUT=%2
    SHIFT & SHIFT
    GOTO :loop
  ) ELSE IF "%1" == "-q" (
    SET QUIET=1
    SHIFT
    GOTO :loop
  ) ELSE (
    REM error, print help and exit
    CALL :help
    EXIT /B 1
  )
)

REM validate the options
IF NOT [%URL%] == [] (
  IF NOT [%HOSTARG%] == [] (
    ECHO Error: you cannot supply both a URL and the -h host option
    EXIT /B 1
  ) ELSE IF NOT [%PORTARG%] == [] (
    ECHO Error: you cannot supply both a URL and the -p port option
    EXIT /B 1
  ) ELSE IF NOT [%USERARG%] == [] (
    ECHO Error: you cannot supply both a URL and the -u user option
    EXIT /B 1
  ) ELSE IF NOT [%PASSARG%] == [] (
    ECHO Error: you cannot supply both a URL and the -s password option
    EXIT /B 1
  ) ELSE IF %SECURE% neq 0 (
    ECHO Error: you cannot supply both a URL and the -S ssl flag
    EXIT /B 1
  ) ELSE IF NOT [%PRINCIPAL%] == [] (
    ECHO Error: you cannot supply both a URL and the -k principal option
    EXIT /B 1
  ) ELSE IF NOT [%KEYTAB%] == [] (
    ECHO Error: you cannot supply both a URL and the -K keytab option
    EXIT /B 1
  )
)

REM TODO: check password and passarg and prompt

REM check for jdk1.8 and exit if not found
SET JAVA_EXEC=java
java -version 1>nul 2>nul || (
   IF NOT DEFINED JAVA_HOME (
     ECHO no java found. %SCRIPT_NAME% requires java.
     REM error, print help and exit
     CALL :help
     EXIT /B 1
   ) ELSE (
     SET JAVA_EXEC="%JAVA_HOME%\bin\java"
   )
)
FOR /f tokens^=2-5^ delims^=.-_^" %%j IN ('java -fullversion 2^>^&1') DO @set "JVER=%%j%%k"
IF %JVER% LSS 18 (
   ECHO Error: java is older than 1.8. %SCRIPT_NAME% requires java 1.8
   REM error, print help and exit
   CALL :help
   EXIT /B 1
)

REM set splice lib dir, for Windows we assume it is always local

SET SCRIPT_DIR=%SCRIPT_DIR:"=%
SET SPLICE_LIB_DIR=%SCRIPT_DIR%lib

REM check if all the tools needed to connect are found
IF NOT EXIST "%SPLICE_LIB_DIR%" (
    ECHO Error: the dependency directory at %SPLICE_LIB_DIR% cannot be found or created.
    REM error, print help and exit
    CALL :help
    EXIT /B 1
)

IF NOT EXIST "%SPLICE_LIB_DIR%\db-client*.jar" (
    ECHO Error: the db-client tool required to connect cannot be found or created.
    REM error, print help and exit
    CALL :help
    EXIT /B 1
)
IF NOT EXIST "%SPLICE_LIB_DIR%\db-tools*.jar" (
    ECHO Error: the db-tools required to connect cannot be found or created.
    REM error, print help and exit
    CALL :help
    EXIT /B 1
)

SET CLASSPATH="%SPLICE_LIB_DIR%/*;";

IF NOT [%HOSTARG%] == [] (
   SET HOST=%HOSTARG%
)
IF NOT [%PORTARG%] == [] (
   SET PORT=%PORTARG%
)
IF NOT [%USERARG%] == [] (
   SET USER=%USERARG%
)

REM prompt securely for user password
IF %PROMPT% EQU 1 (
    REM use PowerShell to read password securely.
    REM source: https://stackoverflow.com/a/20343074/337194
    powershell -Command "$pword = read-host 'Enter password ' -AsSecureString ; $BSTR=[System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($pword) ; [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)" > .tmp.txt
    SET /p PASS=<.tmp.txt & del .tmp.txt
) ELSE IF NOT [%PASSARG%] == [] (
   SET PASS="%PASSARG%"
)

REM Setup IJ_SYS_ARGS based on input options
SET GEN_SYS_ARGS=-Djava.awt.headless^=true
SET IJ_SYS_ARGS=-Djdbc.drivers^=com.splicemachine.db.jdbc.ClientDriver

REM add width via ij.maximumDisplayWidth
IF %WIDTH% NEQ 128 (
   SET IJ_SYS_ARGS=%IJ_SYS_ARGS% -Dij.maximumDisplayWidth=%WIDTH%
)

IF NOT [%OUTPUT%] == [] (
   REM figure out if OUTPUT directory exists
   CALL :dir_from_path OUTPATH !OUTPUT!
   IF NOT EXIST "%OUTPATH%" (
     ECHO Error: you specified a non-existant directory for output %OUTPUT%
     EXIT /B 1
   )
   SET IJ_SYS_ARGS=%IJ_SYS_ARGS% -Dij.outfile^=%OUTPUT%
)

IF NOT [%URL%] == [] (
    SET IJ_SYS_ARGS=%IJ_SYS_ARGS% -Dij.connection.splice^=%URL%
) ELSE (
    REM Add optional URL parameters for SSL and Kerberos
    SET SSL=
    SET KERBEROS=
    IF %SECURE% EQU 1 (
        SET SSL^=;ssl=basic
    )
    IF NOT [%PRINCIPAL%] == [] (
        SET KERBEROS=;principal^=%PRINCIPAL%
        IF NOT [%KEYTAB%] == [] (
            SET KERBEROS=!KERBEROS!;keytab^=%KEYTAB%
        )
    )
    SET IJ_SYS_ARGS=%IJ_SYS_ARGS% -Dij.connection.splice^=jdbc:splice://%HOST%:%PORT%/splicedb!SSL!!KERBEROS!
    SET JAVA_TOOL_OPTIONS=-Dij.user^=%USER% -Dij.password^=%PASS%
    IF %QUOTE% EQU 1 (
        SET JAVA_TOOL_OPTIONS=-Dij.user^='"%USER%"' -Dij.password^=%PASS%
    )
)

IF NOT [%CLIENT_SSL_KEYSTORE%] == [] (
    SET SSL_ARGS=-Djavax.net.ssl.keyStore^=%CLIENT_SSL_KEYSTORE% ^
    -Djavax.net.ssl.keyStorePassword^=%CLIENT_SSL_KEYSTOREPASSWD% ^
    -Djavax.net.ssl.trustStore^=%CLIENT_SSL_TRUSTSTORE% ^
    -Djavax.net.ssl.trustStore.ssl.trustStorePassword^=%CLIENT_SSL_TRUSTSTOREPASSWD%
)

call :message Running Splice Machine SQL shell
call :message For help%: "splice> help;"
REM todo: catch stderr messages and filter out Picked up JAVA_TOOL_OPTIONS   2>nul
java -classpath %CLASSPATH% %GEN_SYS_ARGS% %SSL_ARGS% %IJ_SYS_ARGS% com.splicemachine.db.tools.ij %SCRIPT% 2>nul

CALL :eof
EXIT /B 0

REM write something considering QUIET parameter
:message
   IF %QUIET% EQU 0 (
       ECHO %*
   )
   EXIT /B

REM get directory path from file path
:dir_from_path <resultVarOUT> <pathVarIN>
(
    SET "%~1=%~dp2"
    EXIT /B
)

:help
  ECHO Splice Machine SQL client wrapper script
  ECHO Usage: %SCRIPT_NAME% [-U url] [-h host] [-p port] [-u user] [-Q] [-s pass] [-P] [-S] [-k principal] [-K keytab] [-w width] [-f script] [-o output] [-q]
  ECHO  -U url       full JDBC URL for Splice Machine database
  ECHO  -h host      IP address or hostname of Splice Machine (HBase RegionServer)
  ECHO  -p port      Port which Splice Machine is listening on, defaults to 1527
  ECHO  -u user      username for Splice Machine database
  ECHO  -Q       Quote the username, e.g. for users with . - or @ in the username. e.g. dept-first.last@@company.com
  ECHO  -s pass      password for Splice Machine database
  ECHO  -P       prompt for unseen password
  ECHO  -S       use ssl=basic on connection
  ECHO  -k principal     kerberos principal (for kerberos)
  ECHO  -K keytab    kerberos keytab - requires principal
  ECHO  -w width     output row width. defaults to 128
  ECHO  -f script    sql file to be executed
  ECHO  -o output    file for output
  EXIT /B
:endhelp

:eof
ENDLOCAL
