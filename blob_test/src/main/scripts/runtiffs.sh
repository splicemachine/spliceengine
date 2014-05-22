#!/bin/bash

# For full description, run $ runtiffs.sh -h

TIFF_FILE_EXT=".tiff"
# add this to front of cmd line below to override file extension for tiff files.
# Run $ runtiffs.sh -h to see default
OVERRIDE_TIFF_EXT_ARG="-e ${TIFF_FILE_EXT}"

export  CLASSPATH="../lib/*"

java "com.splicemachine.blob.RunTiffBlobs" $*
