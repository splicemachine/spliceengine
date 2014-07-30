#!/bin/sh
#
# Used by Jenkins to upload jars to Nexus.
#
# Ultimately we should be able to replace this entire script with 'mvn install deploy'. Maybe after we extract a
# company pom from the speliceengine project and us it as the parent of this project.
#

REPO_URL="http://nexus.splicemachine.com/nexus/content/repositories/releases/"
REPO_ID="splicemachine"
VER="10.9.1.0-splice"
GROUPID="org.apache.derby"

CMD_PREFIX="mvn -B deploy:deploy-file -Durl=${REPO_URL} -DgroupId=${GROUPID} -Dversion=${VER} -Dpackaging=jar -DrepositoryId=${REPO_ID}"

mvn -B clean install

${CMD_PREFIX} -Dfile=client/target/derbyclient-${VER}.jar                   -DartifactId=derbyclient
${CMD_PREFIX} -Dfile=client/target/derbyclient-${VER}-sources.jar           -DartifactId=derbyclient -Dclassifier=sources

${CMD_PREFIX} -Dfile=engine/target/derby-${VER}.jar                         -DartifactId=derby
${CMD_PREFIX} -Dfile=engine/target/derby-${VER}-sources.jar                 -DartifactId=derby -Dclassifier=sources

${CMD_PREFIX} -Dfile=net/target/derbynet-${VER}.jar                         -DartifactId=derbynet
${CMD_PREFIX} -Dfile=net/target/derbynet-${VER}-sources.jar                 -DartifactId=derbynet -Dclassifier=sources

${CMD_PREFIX} -Dfile=tools/target/derbytools-${VER}.jar                     -DartifactId=derbytools
${CMD_PREFIX} -Dfile=tools/target/derbytools-${VER}-sources.jar             -DartifactId=derbytools -Dclassifier=sources

${CMD_PREFIX} -Dfile=run/target/derbyrun-${VER}.jar                         -DartifactId=derbyrun
${CMD_PREFIX} -Dfile=testing/target/derbyTesting-${VER}.jar                 -DartifactId=derbyTesting

${CMD_PREFIX} -Dfile=derbyLocale_cs/target/derbyLocale_cs-${VER}.jar        -DartifactId=derbyLocale_cs
${CMD_PREFIX} -Dfile=derbyLocale_de_DE/target/derbyLocale_de_DE-${VER}.jar  -DartifactId=derbyLocale_de_DE
${CMD_PREFIX} -Dfile=derbyLocale_es/target/derbyLocale_es-${VER}.jar        -DartifactId=derbyLocale_es
${CMD_PREFIX} -Dfile=derbyLocale_fr/target/derbyLocale_fr-${VER}.jar        -DartifactId=derbyLocale_fr
${CMD_PREFIX} -Dfile=derbyLocale_hu/target/derbyLocale_hu-${VER}.jar        -DartifactId=derbyLocale_hu
${CMD_PREFIX} -Dfile=derbyLocale_it/target/derbyLocale_it-${VER}.jar        -DartifactId=derbyLocale_it
${CMD_PREFIX} -Dfile=derbyLocale_ja_JP/target/derbyLocale_ja_JP-${VER}.jar  -DartifactId=derbyLocale_ja_JP
${CMD_PREFIX} -Dfile=derbyLocale_ko_KR/target/derbyLocale_ko_KR-${VER}.jar  -DartifactId=derbyLocale_ko_KR
${CMD_PREFIX} -Dfile=derbyLocale_pl/target/derbyLocale_pl-${VER}.jar        -DartifactId=derbyLocale_pl
${CMD_PREFIX} -Dfile=derbyLocale_pt_BR/target/derbyLocale_pt_BR-${VER}.jar  -DartifactId=derbyLocale_pt_BR
${CMD_PREFIX} -Dfile=derbyLocale_ru/target/derbyLocale_ru-${VER}.jar        -DartifactId=derbyLocale_ru
${CMD_PREFIX} -Dfile=derbyLocale_zh_CN/target/derbyLocale_zh_CN-${VER}.jar  -DartifactId=derbyLocale_zh_CN
${CMD_PREFIX} -Dfile=derbyLocale_zh_TW/target/derbyLocale_zh_TW-${VER}.jar  -DartifactId=derbyLocale_zh_TW
