#!/bin/bash
cd `dirname $0`/..

if [ -z "${SONATYPE_USERNAME}" ]
then
    echo "ERROR! Please set SONATYPE_USERNAME and SONATYPE_PASSWORD environment variable"
    exit 1
fi

if [ -z "${SONATYPE_PASSWORD}" ]
then
    echo "ERROR! Please set SONATYPE_PASSWORD environment variable"
    exit 1
fi

if [ ! -z "${GPG_SECRET_KEYS}" ]
then
    echo ${GPG_SECRET_KEYS} | base64 --decode | ${GPG_EXECUTABLE} --import
fi

if [ ! -z "${GPG_OWNERTRUST}" ]
then
    echo ${GPG_OWNERTRUST} | base64 --decode | ${GPG_EXECUTABLE} --import-ownertrust
fi

if [ -n "${TRAVIS_TAG}" ]
then
    echo "travis tag is set, applying gpg signing"
    GPG_SKIP=false
    mvn org.codehaus.mojo:versions-maven-plugin:2.5:set -DnewVersion=$TRAVIS_TAG
else
    echo "no travis tag is set, skipping gpg signing"
    GPG_SKIP=true
fi

mvn clean deploy --settings .travis/settings.xml -DskipGpg=${GPG_SKIP} -DskipTests=true -B -U
SUCCESS=$?

if [ ${SUCCESS} -eq 0 ]
then
    echo "successfully deployed the jars to nexus"
fi

exit ${SUCCESS}

