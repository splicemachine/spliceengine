#!/usr/local/bin/bash
#This script checks mismatched build numbers of pom.xml.
#Sample usage: 
# ./checkBuildNumber spliceengine
# ./checkBuildNumber spliceengine-ee

display_usage() { 
   echo -e "\nUsage:\n./checkBuildNumber <folderName> \n" 
} 
if [  $# -ne 1 ] 
then 
   display_usage
   exit 1
fi 
 
folder=$1

if [ ! -d $1 ]; then 
   echo "$folder does not exist."
   exit 1
fi

string=`find $folder -name pom.xml | xargs grep SNAPSHOT | awk -F: '{print $1 $2}' | sed 's/[[:space:]][[:space:]]*/ /g' | sort -k 2`

IFS=$'\n' read -rd '' -a list <<<"$string"

declare -A dict

for element in "${list[@]}"
do
   stringArray=($element)
   dict[${stringArray[1]}]=${stringArray[0]}
done

if [ ${#dict[@]} == 1 ]; then
   echo -e "No mismatched build numbers in pom.xml files. All good !!!"
   exit 0
else
   echo -e "Mismatched build numbers in pom.xml files:"
   for key in "${!dict[@]}"
   do
      echo ${dict[$key]}  $key
   done
fi



