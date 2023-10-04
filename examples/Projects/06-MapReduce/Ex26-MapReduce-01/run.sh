#!/bin/bash

clear

source ./usage.sh

#
# Configuration files
#
JOB_CONF=""
for conf in "$@"; do
	JOB_CONF="${JOB_CONF} -conf ${conf}"
done

CORPUS_NAME=gutenberg-small

INPUT_DIRECTORY=${HOME}/examples/input/${CORPUS_NAME}
OUTPUT_DIRECTORY=${HOME}/examples/output/${CORPUS_NAME}

echo -e "Removing previous output..."
CMD="rm -rf ${OUTPUT_DIRECTORY}"
echo -e "${CMD}"
${CMD}

INPUT=file://${INPUT_DIRECTORY}
OUTPUT=file://${OUTPUT_DIRECTORY}

ARGS="${JOB_CONF} ${INPUT} ${OUTPUT}"

echo -e "\nExporting classpath..."
CMD="export HADOOP_CLASSPATH=${JAR_FILE}"
echo -e "${CMD}"
${CMD}

echo -e "\nRunning..."
CMD="hadoop ${MAIN_CLASS} ${ARGS}"
echo -e "${CMD}\n"

${CMD} 2>&1

echo "Press ENTER to view result..."
read x

CMD="cat ${OUTPUT_DIRECTORY}/part-r-00000"
echo -e "${CMD}\n"
${CMD}
