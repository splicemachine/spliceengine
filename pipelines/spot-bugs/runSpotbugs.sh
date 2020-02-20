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

errors=

echo "Running spotbugs..."
time mvn -e spotbugs:check -Pcore,$platform,mem,ee --fail-never | tee out_file
for file in $(git diff $(git merge-base $branch HEAD) --name-only); do
    if new_errors="$(grep "$(basename $file)" out_file)"
    then
        errors+=$'\n'
        errors+="$new_errors"
    fi
done
rm out_file

count=$(grep -c "ERROR" <(echo "$errors"))

echo ""
echo "#######################################################"
echo "$count specific errors to fix before this PR can be merged:"
echo "$errors"
echo "#######################################################"
echo ""
echo "To reproduce locally, run ./pipelines/spot-bugs/runSpotbugs.sh $platform $branch"

exit $count
