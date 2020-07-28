#!/bin/bash

mvn -q -Pcore,cdh6.3.0 -f ./splice_ck/pom.xml exec:java -Dexec.args="$*"

