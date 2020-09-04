#!/bin/bash

# first things first, we need to be root since we're replacing package/parcel files
whoami | grep -q ^root$ || {
  echo "please run this as root"
  exit 1
}

# timestamp
TS="$(date '+%Y%m%d%H%M%S')"

# supported platforms
declare -a platforms
platforms=( cdh hdp mapr )

# our host doesn't have a platform by default
declare -A thishost
for platform in ${platforms[@]} ; do
  thishost[${platform}]=""
done

# where we link sqlshell.sh to
sqlshellshdir="/usr/bin"

# top-level directories to check
#   these are true for now
#   cm/cdh assumes installation from parcels *only*
declare -A topdir
topdir[cdh]="/opt/cloudera"
topdir[hdp]="/usr/hdp"
topdir[mapr]="/opt/mapr"

# our directories, per platform
declare -A splicedir
splicedir[cdh]="${topdir[cdh]}/parcels/SPLICEMACHINE/"
splicedir[hdp]="/opt/splice/default/"
splicedir[mapr]="/opt/splice/default/"

# figure out what platform(s) we're running on
for platform in ${platforms[@]} ; do
  test -e ${topdir[${platform}]} && eval $(echo thishost[${platform}]="0")
done

# loop through platforms and start doing work
for platform in ${platforms[@]} ; do
  # only run if we actually discovered an installation for this platform
  if [[ -n "${thishost[${platform}]}" ]] ; then

    # verify we have an existing splice install
    if [[ ! -e "${splicedir[${platform}]}" ]] ; then
      echo "host seems to be ${platform} but Splice Machine directory ${splicedir[${platform}]} not found"
      continue
    fi

    # we need an uber jar - bail if we don't have one
    spliceuberjar=""
    spliceuberjar="$(find ${splicedir[${platform}]} -xdev -type f -name splice\*uber.jar | head -1)"
    # bail if we don't find one
    if [[ -z "${spliceuberjar}" ]] ; then
      echo "did not find a Splice Machine uber jar under ${splicedir[${platform}]}"
      continue
    fi
    echo "Splice Machine uber jar is ${spliceuberjar}"

    # same thing for yarn
    spliceyarnwebproxyjar=""
    spliceyarnwebproxyjar="$(find ${splicedir[${platform}]} -xdev -type f -name splice\*yarn-webproxy.jar | head -1)"
    if [[ -z "${spliceyarnwebproxyjar}" ]] ; then
      echo "did not find a Splice Machine YARN jar under ${splicedir[${platform}]}"
      continue
    fi
    echo "Splice Machine YARN jar is ${spliceyarnwebproxyjar}"

    # sqlshell.sh - needed for hdp/mapr
    sqlshellsh=""
    sqlshellsh="$(find ${splicedir[${platform}]} -xdev -type f -name sqlshell.sh | head -1)"
    # bail if we don't find one
    if [[ -z "${sqlshellsh}" ]] ; then
      echo "did not find a Splice Machine sqlshell.sh under ${splicedir[${platform}]}"
      continue
    fi
    echo "Splice Machine sqlshell.sh is ${sqlshellsh}"

    splicecksh=""
    splicecksh="$(find ${splicedir[${platform}]} -xdev -type f -name spliceck.sh | head -1)"
    # bail if we don't find one
    if [[ -z "${splicecksh}" ]] ; then
      echo "did not find a Splice Machine spliceck.sh under ${splicedir[${platform}]}"
      continue
    fi
    echo "Splice Machine spliceck.sh is ${splicecksh}"

    # ws.rs-api jar
    splicewsrsapijar=""
    splicewsrsapijar="$(find ${splicedir[${platform}]} -xdev -type f -name javax.ws.rs-api\*.jar | head -1)"
    if [[ -z "${splicewsrsapijar}" ]] ; then
        echo "did not find javax.ws.rs-api jar under ${splicedir[${platform}]}"
        continue
    fi
    echo "Splice Machine javax.ws.rs-api is ${splicewsrsapijar}"

    # and servlet-api >= 3.1.0
    spliceservletapijar=""
    spliceservletapijar="$(find ${splicedir[${platform}]} -xdev -type f -name \*servlet-api\*.jar | head -1)"
    if [[ -z "${spliceservletapijar}" ]] ; then
        echo "did not find servlet-api jar under ${splicedir[${platform}]}"
        continue
    fi
    echo "Splice Machine servlet-api jar is ${spliceservletapijar}"

    # scan the platform top-level directory for *real* files matching servlet-api-2
    # we'll backup and replace these with symbolink links
    declare -a servletapijars
    servletapijars=""
    servletapijars="$(find ${topdir[${platform}]} -xdev -type f \( -name \*servlet-api-2\*.jar -o -name servlet-api.jar -o -name javax.servlet-2.5\*.jar \) )"
    for servletapijar in ${servletapijars[@]} ; do
      echo "backing up ${servletapijar} to ${servletapijar}.PRE-${TS}"
      mv ${servletapijar}{,.PRE-${TS}}
      echo "symlinking ${spliceservletapijar} to ${servletapijar}"
      ln -sf ${spliceservletapijar} ${servletapijar}
    done

    # make sure all jars are fully world-readable
    for splicejar in $(find ${splicedir[${platform}]} -xdev -type f -name \*.jar) ; do
      echo "setting full read permissions (644) for ${splicejar}"
      chmod 644 ${splicejar}
    done

    # make sure all scripts are executable
    for splicescript in $(find ${splicedir[${platform}]} -xdev -type f -name \*.sh) ; do
      echo "setting full execute permissions (755) for ${splicescript}"
      chmod 755 ${splicescript}
    done

    # platform-specific stuff
    # we need a list of lib directories where we can symlink our files so yarn can use them
    yarnlibdirs=""
    if [[ ${platform} =~ ^hdp$ ]] ; then
      yarnlibdirs="$(ls -d ${topdir[${platform}]}/*/hadoop-yarn/lib/)"
    elif [[ ${platform} =~ ^mapr$ ]] ; then
      yarnlibdirs="$(ls -d ${topdir[${platform}]}/hadoop/*/share/hadoop/yarn/)"
    elif [[ ${platform} =~ ^cdh$ ]] ; then
      yarnlibdirs="$(ls -d ${topdir[${platform}]}/parcels/CDH/lib/hadoop-yarn/ ${topdir[${platform}]}/parcels/CDH/lib/hadoop-yarn/lib/)"
    fi

    # on cdh, we need to move spark 1.x jars out of the way
    # XXX - will this break Spark 2 (beta) parcel/CSD? probably
    # XXX - already breaks everything else, so...
    if [[ ${platform} =~ ^cdh$ ]] ; then
      jardir="${topdir[${platform}]}/parcels/CDH/jars/"
      disjardir="${topdir[${platform}]}/parcels/CDH/jars.OFF/"
      test -e ${disjardir} || mkdir -p ${disjardir}
      for cdhsparkjar in $(find ${jardir} -type f | egrep '/(spark|hbase-spark).*\.jar$') ; do
        echo "moving ${cdhsparkjar} to ${disjardir}"
        mv ${cdhsparkjar} ${disjardir}
      done
    fi

    # symlink sqlshell.sh and spliceck.sh to the right place
    if [[ ${platform} =~ ^hdp$ || ${platform} =~ ^mapr$ ]] ; then
      echo "symlinking ${sqlshellsh} to ${sqlshellshdir}"
      ln -sf ${sqlshellsh} ${sqlshellshdir}
    fi
    echo "symlinking ${splicecksh} to ${sqlshellshdir}"
    ln -sf ${splicecksh} ${sqlshellshdir}

    # loop through our list, clean up and re-link everything.
    # XXX - can likely remove platform check conditional here
    if [[ ${platform} =~ ^hdp$ || ${platform} =~ ^mapr$ || ${platform} =~ ^cdh$ ]] ; then
      for yarnlibdir in ${yarnlibdirs} ; do
        # clean up first
        echo "finding and removing any Splice Machine symlinks from ${yarnlibdir}"
        splicesymlinks=""
        splicesymlinks="$(find ${yarnlibdir} -xdev -type l \( -name spark-assembly-\*.jar -o -name splice\*.jar \))"
        for splicesymlink in ${splicesymlinks} ; do
          file ${splicesymlink} | grep -q ${splicedir[${platform}]}
          if [[ ${?} -eq 0 ]] ; then
            echo "removing ${splicesymlink}"
            rm -f ${splicesymlink}
          fi
        done
        # now symlink in our uber and yarn jars
        for symlinkjar in ${spliceuberjar} ${spliceyarnwebproxyjar} ${splicewsrsapijar} ; do
          echo "symlinking ${symlinkjar} into ${yarnlibdir}"
          ln -sf ${symlinkjar} ${yarnlibdir}
        done
      done
    fi
  fi
done
