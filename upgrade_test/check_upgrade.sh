#!/bin/bash

VERSION=3.1.0.1971

# make sure we're in git root
gitroot=$(git rev-parse --show-toplevel)
pushd ${gitroot}

# download the previous standalone data
aws s3 cp s3://splice-snapshots/upgrade_tests/platform_it_${VERSION}.tar.gz .
tar -xzf platform_it_${VERSION}.tar.gz
rm platform_it_${VERSION}.tar.gz
