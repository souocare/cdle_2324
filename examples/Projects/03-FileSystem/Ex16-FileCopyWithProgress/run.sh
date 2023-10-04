#!/bin/bash

source ./usage.sh > /dev/null

FILE=1-0.txt
DIRECTORY_FILE=examples/input/gutenberg

LOCAL_DIRECTORY=/home/${USER}/${DIRECTORY_FILE}
REMOTE_DIRECTORY=/user/${USER}/${DIRECTORY_FILE}

LOCAL_FILE=${LOCAL_DIRECTORY}/${FILE}
REMOTE_FILE=hdfs://${REMOTE_DIRECTORY}/${FILE}

echo -e "\nCreating remote directory..."
CMD="hadoop fs -mkdir -p ${REMOTE_DIRECTORY}"
echo -e "\n${CMD}"
${CMD}

echo -e "\nDeleting remote file..."
CMD="hadoop fs -rm ${REMOTE_FILE}"
echo -e "\n${CMD}"
${CMD}

ARGS="${LOCAL_FILE} ${REMOTE_FILE} $1"

CMD="export HADOOP_CLASSPATH=${JAR_FILE}"
echo -e "\n${CMD}"
${CMD}

CMD="hadoop ${MAIN_CLASS} ${ARGS}"
echo -e "\n${CMD}\n"
${CMD}
