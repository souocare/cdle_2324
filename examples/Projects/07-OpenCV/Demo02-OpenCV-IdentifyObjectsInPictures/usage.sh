#!/bin/bash

#
# Setting Hadoop class path
#
HADOOP_COMPILE_CLASSPATH=`hadoop classpath`

CURRENT_DIRECTORY=.

SRC_DIR=${CURRENT_DIRECTORY}/src/main/java
JAR_FILE=`ls ${CURRENT_DIRECTORY}/target/*.jar`

#
# Identifying main class
#
cd ${SRC_DIR}
MAIN_CLASS_AUX=`find . -type f -exec grep -H 'public static void main' {} \; | cut -d ':' -f 1 | cut -d '.' -f 2`
cd ${CURRENT_DIRECTORY}

MAIN_CLASS=`echo ${MAIN_CLASS_AUX:1} | tr '/' '.'`

function showUsage() {
	if [ "${MAIN_CLASS}" != "" ]; then
		echo -e "\nUsage:"
		echo -e "export HADOOP_CLASSPATH=${JAR_FILE}"
		echo -e "hadoop ${MAIN_CLASS} <args>\n"
	else
		echo -e "\nMain class not detected!"
		echo -e "\nRunning a library?\n"
	fi
}

showUsage