#!/bin/bash

clear

source ./usage.sh

INPUT_DIRECTORY=${HOME}/examples/input/temperatures/s1-s2/
OUTPUT_DIRECTORY=${HOME}/examples/output/temperatures/s1-s2

CURRENT_DIRECTORY=`pwd`
ADDITIONAL_CLASSPATH_DIRECTORY=${CURRENT_DIRECTORY}/../Ex04-Temperatures-03-Lib/target
JOB_ADDITIONAL_CLASSPATH=`ls ${ADDITIONAL_CLASSPATH_DIRECTORY}/*.jar`

echo -e "Removing previous output..."
CMD="rm -rf ${OUTPUT_DIRECTORY}"
echo -e "${CMD}"
${CMD}

INPUT=file://${INPUT_DIRECTORY}
OUTPUT=file://${OUTPUT_DIRECTORY}

JOB_CLASSPATH="-libjars ${JOB_ADDITIONAL_CLASSPATH}"

JOB_CONF=${JOB_CLASSPATH}

ARGS="${JOB_CONF} ${INPUT} ${OUTPUT}"

echo -e "\nExporting classpath..."
CMD="export HADOOP_CLASSPATH=${JAR_FILE}"
echo -e "${CMD}"
${CMD}

echo -e "\nRunning..."
CMD="hadoop ${MAIN_CLASS} ${ARGS}"
echo -e "${CMD}\n"

${CMD}

echo "Press ENTER to view result..."
read x

clear
CMD="cat ${OUTPUT_DIRECTORY}/part-r-00000"
echo -e "${CMD}\n"
${CMD}
