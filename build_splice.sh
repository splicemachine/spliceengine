# call with your gpg passphrase as the first param

ant clean
mkdir bin/templates
ant buildsource buildjars derbytestingjar
cd maven2
./SpliceBuild.sh $1
cd ..
