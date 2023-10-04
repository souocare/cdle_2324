#!/bin/bash

source ./usage.sh > /dev/null

ARGS=file:///home/usermr/examples/Projects/pom.xml

CMD="export HADOOP_CLASSPATH=${JAR_FILE}"
echo -e "\n${CMD}"
${CMD}

CMD="hadoop ${MAIN_CLASS} ${ARGS}"
echo -e "\n${CMD}\n"
${CMD}
