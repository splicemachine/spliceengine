# call with your gpg passphrase as the first param

ant clean && mkdir -p bin/templates && ant buildsource testing buildjars && cd maven2 && mvn clean install && cd ..
