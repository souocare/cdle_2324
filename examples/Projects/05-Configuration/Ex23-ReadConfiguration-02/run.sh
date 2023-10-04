#!/bin/bash

source ./usage.sh > /dev/null

ARGS=`pwd`

CMD="export HADOOP_CLASSPATH=${JAR_FILE}:${CURRENT_DIRECTORY}/../lib/*.jar"
echo -e "\n${CMD}"
${CMD}

hadoop ${MAIN_CLASS} ${ARGS}
