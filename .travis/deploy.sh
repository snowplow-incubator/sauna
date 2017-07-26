#!/bin/bash

tag=$1

release=${tag:0}

if [ "${release}" == "" ]; then
    echo "Warning! No release specified! Ignoring."
    exit 2
fi

cd $TRAVIS_BUILD_DIR

export TRAVIS_BUILD_RELEASE_TAG=${release}
release-manager --config ./.travis/release_sauna.yml --check-version --make-version --make-artifact --upload-artifact
