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
    spliceyarnjar=""
    spliceyarnjar="$(find ${splicedir[${platform}]} -xdev -type f -name splice\*yarn\*.jar | head -1)"
    if [[ -z "${spliceyarnjar}" ]] ; then
      echo "did not find a Splice Machine YARN jar under ${splicedir[${platform}]}"
      continue
    fi
    echo "Splice Machine YARN jar is ${spliceyarnjar}"

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
    servletapijars="$(find ${topdir[${platform}]} -xdev -type f \( -name \*servlet-api-2\*.jar -o -name servlet-api.jar \) )"
    for servletapijar in ${servletapijars[@]} ; do
      echo "backing up ${servletapijar} to ${servletapijar}.PRE-${TS}"
      mv ${servletapijar}{,.PRE-${TS}}
      echo "symlinking ${spliceservletapijar} to ${servletapijar}"
      ln -sf ${spliceservletapijar} ${servletapijar}
    done

    # XXX - any CDH platform-specific steps?

    # platform-specific stuff for hdp, mapr
    # we need a list of lib directories where we can symlink our files so yarn can use them
    yarnlibdirs=""
    if [[ ${platform} =~ ^hdp$ ]] ; then
      yarnlibdirs="$(ls -d ${topdir[${platform}]}/*/hadoop-yarn/lib/)"
    elif [[ ${platform} =~ ^mapr$ ]] ; then
      yarnlibdirs="$(ls -d ${topdir[${platform}]}/hadoop/*/share/hadoop/yarn/)"
    fi

    # loop through our list, clean up and re-link everything.
    if [[ ${platform} =~ ^hdp$ || ${platform} =~ ^mapr$ ]] ; then
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
        for symlinkjar in ${spliceuberjar} ${spliceyarnjar} ; do
          echo "symlinking ${symlinkjar} into ${yarnlibdir}"
          ln -sf ${symlinkjar} ${yarnlibdir}
        done
      done
    fi
  fi
done
