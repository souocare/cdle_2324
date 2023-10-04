#!/bin/bash

if [[ $# -lt 2 ]]; then
	echo "Invalid arguments!"
	echo "Usage:"
	echo "$0 <firts year> <last year>"
	exit
fi

source ./usage.sh

BASE_DIR=/${HOME}/examples

IN_NAME=input/temperatures/$1-$2
OUT_NAME=output/temperatures/$1-$2

INPUT_DIRECTORY=file://${BASE_DIR}/${IN_NAME}
OUTPUT_DIRECTORY=file://${BASE_DIR}/${OUT_NAME}

ARGS="${INPUT_DIRECTORY} ${OUTPUT_DIRECTORY}"

echo "Removing previous output..."
rm -rf ${BASE_DIR}/${OUT_NAME}

echo "Exporting classpath..."
export HADOOP_CLASSPATH=${JAR_FILE}

echo "HADOOP_CLASSPATH=${HADOOP_CLASSPATH}"

echo "Running..."
CMD="hadoop ${MAIN_CLASS} ${ARGS}"
echo ${CMD}

${CMD}

echo "Press ENTER to view result..."
read x

clear
zcat ${BASE_DIR}/${OUT_NAME}/part-r-00000.gz
