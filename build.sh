#!/bin/bash

################################################################################
# Build derby (db) for splice developers. 
#
# There are a number of options which are known to work here:
#
# 1. clean (-c or --clean) Cleans existing data
# 2. test (-t or --buildtest) builds test jars
# 3. sane (-s <sanity> or --sanity=<sanity>). When <sanity>=true, then a "sane"
# 			build is invoked, otherwise an "insane" build is invoked. For more
#				information about sane vs. insane builds, see BUILDING.HTML
# 4. deploy (-d or --deploy). When set, this will attempt to deploy
#        jars using "maven install" like commands
# 4. help (-h or --help) will print a usage message.

################################################################################

function check_sanity_arg() 
{
						if [ -n "$1" ]; then
										echo "SANITY argument is set twice!"
										exit 65
						fi
}

function print_usage()
{
				HELP_MESSAGE="Usage: build.sh [args]\n"
				HELP_MESSAGE="${HELP_MESSAGE}Arguments:\n"
				HELP_MESSAGE="${HELP_MESSAGE}\t-h|--help\t\t\t Prints this usage message\n"
				HELP_MESSAGE="${HELP_MESSAGE}\t-c|--clean\t\t\t Clean previous build data before running\n"
				HELP_MESSAGE="${HELP_MESSAGE}\t-t|--test\t\t\t Include testing jars in build\n"
				HELP_MESSAGE="${HELP_MESSAGE}\t-d|--deploy\t\t\t Deploy the built jars to local maven repository\n"
				HELP_MESSAGE="${HELP_MESSAGE}\t-l|--locale\t\t\t Deploy locale-specific jars to local maven repository. This is disabled by default\n"
				HELP_MESSAGE="${HELP_MESSAGE}\t-s <sanity>\t\t\t  Specify whether to perform a 'sane' or 'insane' build. By default, we use sane build\n"
				HELP_MESSAGE="${HELP_MESSAGE}\t-sanity=<sanity>\t\t Same as -s <sanity>.\n"
				HELP_MESSAGE="${HELP_MESSAGE}-----------------\n"

				echo -e $HELP_MESSAGE
}

while [[ $# > 0 ]]; do
	arg=$1
	case $arg in
		-h|--help)
						print_usage
						exit 0
						;;
		-c|--clean)
						CLEAN="clean"
						shift
						;;
		-t|--test)
						TEST="testing"
						shift
						;;
		-d|--deploy)
						DEPLOY="install"
						shift
						;;
		-l|--locale)
						LOCALE="locale"
						shift
						;;
		-s)
						check_sanity_arg $SANITY
						shift
						SANITY=$1
						shift
						;;
		--sanity=*)
						check_sanity_arg $SANITY
						SANITY="${arg#*=}"
						shift
						;;
		*)
						echo "Unknown arg $arg"
						print_usage
						exit 65
	esac
done

#By default, we build SANE, so if SANITY isn't set, set it to sane
if [ -z "$SANITY" ]; then 
				SANITY="sane"
fi

ANT_COMMAND=""
if [ -n "$CLEAN" ]; then
				ANT_COMMAND="${ANT_COMMAND} clean"
fi
ANT_COMMAND="${ANT_COMMAND} buildsource"
if [ -n "$TEST" ]; then
				ANT_COMMAND="${ANT_COMMAND} testing"
fi
ANT_COMMAND="${ANT_COMMAND} buildjars"

shopt -s nocasematch
if [[ "$SANITY" == "insane" ]]; then
					ANT_COMMAND="${ANT_COMMAND} -Dsane=false"
else
				ANT_COMMAND="${ANT_COMMAND} -Dsane=true"
fi
shopt -u nocasematch

if [ -z "$LOCALE" ]; then
	ANT_COMMAND="${ANT_COMMAND} -Dskip.locale=true"
fi

ant $ANT_COMMAND
CODE=$?

if [ -z "$DEPLOY" ]; then
				exit $CODE
fi

#We need to use maven to deploy this properly
MVN_COMMAND="install"
shopt -s nocasematch
if [[ "$SANITY" == "insane" ]]; then
					MVN_COMMAND="${MVN_COMMAND} -Dsanity=insane" #TODO -sf- this may not work, I think we have to modify sane.properties actually
fi
shopt -u nocasematch

if [ -z "$LOCALE" ]; then
	MVN_COMMAND="${MVN_COMMAND} -pl engine,net,client,tools,run"
	if [ -n "$TEST" ]; then
					MVN_COMMAND="${MVN_COMMAND},testing"
	fi
fi

CURRENT_DIR=$(pwd)
cd maven2 && mvn $MVN_COMMAND && cd $CURRENT_DIR

