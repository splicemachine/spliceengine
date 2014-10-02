#!/bin/bash

# Builds derby for splice developers. 
# Pass in "sane" or "" for typical debug builds
# Pass in "insane" for a production build
# see BUILDING.html for more detail on sane vs. insane builds

if [[ "${1}" == "" || "${1}" == "sane" ]]; then
	ant clean && ant buildsource testing buildjars && cd maven2 && mvn clean install && cd ..
elif [[ "${1}" == "insane" ]]; then
	ant clean && ant buildsource testing buildjars -Dsane=false && cd maven2 && mvn clean install -Dsanity=insane && cd ..
else 
	echo "	USAGE: ${0} [ sane | insane | NULL ] " 
fi
