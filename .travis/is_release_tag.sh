#!/bin/bash

tag=$1
release=${tag:0}

if [ "${release}" == "" ]; then
    echo "Warning! No release specified! Ignoring."
    exit 2
fi
exit 0
