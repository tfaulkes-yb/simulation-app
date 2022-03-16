#!/bin/sh

echo "Setting testEnv flag to be false"
sed 's/testEnv = true;/testEnv = false;/' ./src/app/services/yugabyte-data-source.service.ts > /tmp/yugabyte-data-source.service.ts
if [ "$?" -ne "0" ]
then
	echo "Sed command failed, aborting"
	exit
fi

cp /tmp/yugabyte-data-source.service.ts ./src/app/services/yugabyte-data-source.service.ts
if [ "$?" -ne "0" ]
then
	echo "Copy command failed, aborting"
	exit
fi

ng build
if [ "$?" -ne "0" ]
then
	echo "*** BUILD FAILED ***"
	exit
fi
cp ./dist/workload-simulation-ui-src/*  ../static
if [ "$?" -ne "0" ]
then
	echo "*** COPY FAILED ***"
	exit
fi
cd ../../../..
mvn clean package -Dmaven.test.skip=true

