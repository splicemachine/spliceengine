@echo off

echo Running Splice Machine SQL shell
echo For help: "splice> help;"

if "%CLIENT_SSL_KEYSTORE%"=="" (
  mvn -f splice_machine exec:java
) else (
  mvn -f splice_machine exec:java ^
    -Djavax.net.ssl.keyStore=${CLIENT_SSL_KEYSTORE} ^
    -Djavax.net.ssl.keyStorePassword=${CLIENT_SSL_KEYSTOREPASSWD} ^
    -Djavax.net.ssl.trustStore=${CLIENT_SSL_TRUSTSTORE} ^
    -Djavax.net.ssl.trustStore.ssl.trustStorePassword=${CLIENT_SSL_TRUSTSTOREPASSWD}
)
