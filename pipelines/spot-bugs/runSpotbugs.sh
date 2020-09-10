#!/bin/bash
#This script runs spotbugs and display errors related to files modified by this PR
#Sample usage:
# ./pipelines/spot-bugs/runSpotbugs.sh cdh5.14.0 master

display_usage() {
   echo -e "\nUsage:\n./runSpotbugs <platform> <base_branch>\n"
}
if [  $# -ne 2 ]
then
   display_usage
   exit 1
fi

platform=$1
branch=$2

out_file=$(mktemp)

spotbugs_errors=

echo "Running spotbugs..."
time mvn -B -e --fail-never spotbugs:check -Pcore,$platform,mem,ee | tee out_file
for file in $(git diff origin/$branch...HEAD --name-only); do
    if new_spotbugs_errors="$(grep "\<$(basename $file)\>" out_file)"
    then
        spotbugs_errors+=$'\n'
        spotbugs_errors+="$new_spotbugs_errors"
    fi
done

count=$(grep -c "ERROR" <(echo "$spotbugs_errors"))

# If count is zero, it could be because some spotbugs did not even get to run, so let's make sure that
# we still reached spotbugs errors
if [ $count -eq "0" ]
then
    if grep -qi "build failures were ignored" out_file
    then
        if ! grep "Failed to execute goal com.github.spotbugs.*failed with [0-9]* bugs and [0-9]* errors" out_file
        then
            echo "[ERROR]: Some non spotbugs error occurred."
            rm out_file
            exit 1
        fi
    fi
fi

echo ""
echo "#######################################################"
echo "$count specific errors to fix before this PR can be merged:"
echo "$spotbugs_errors"
echo "#######################################################"
echo ""
echo "To reproduce locally, run mvn -Pcore,$platform,mem,ee -DskipTests clean install && ./pipelines/spot-bugs/runSpotbugs.sh $platform $branch"

rm out_file

exit $count
