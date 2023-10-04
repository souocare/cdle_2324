#!/bin/bash

source ./usage.sh > /dev/null

ARGS=org.apache.hadoop.io.compress.GzipCodec

CMD="export HADOOP_CLASSPATH=${JAR_FILE}"
echo -e "\n${CMD}"
${CMD}

CMD="echo \"This is a test\" | hadoop ${MAIN_CLASS} ${ARGS} | zcat"
echo -e "\n${CMD}\n"

echo "This is a test" | hadoop ${MAIN_CLASS} ${ARGS} | zcat
