#!/bin/sh

#
# Not used?
#

if [ "$#" -eq 1 ]; then
  echo "Running Splice Derby Build"
else
  echo "You did not enter the passphrase for gpg signing" && exit 1	
fi

mvn clean
mvn -Dgpg.passphrase="$1" install
SpliceDeploy2.sh