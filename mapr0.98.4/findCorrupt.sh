#!/bin/sh

######################################################
# Find all the corrupted HFiles in a directory.
#
# Make sure that dump.sh is executable, and that it
# is located in the same directory as this script.
#
######################################################
DIR=$1
#bad regex-fu to determine if the file consists only of hex characters.
#probably doesn't work 100% correctly
HFILES=$(find ${DIR} -name '[0-9a-f]*')

for hfile in $HFILES; do
	#Look at the output and see if the file is reported as corrupt
	OUT=$(./dump.sh ${hfile} 2>/dev/null| grep 'Corrupt')
	if [[ ${OUT} == *[![:space:]] ]]; then
		echo "File ${hfile} is corrupt"
	fi
done
