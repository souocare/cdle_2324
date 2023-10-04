#!/bin/bash

source ./usage.sh > /dev/null

TEMP_DIRECTORY=${HOME}/temp

INPUT_FILE=1-0.txt
INPUT_DIRECTORY=${HOME}/examples/input/gutenberg

INPUT=${INPUT_DIRECTORY}/${INPUT_FILE}

echo "Creating temporary directory (${TEMP_DIRECTORY})..."
CMD="mkdir -p ${TEMP_DIRECTORY}"
echo -e "\n${CMD}"
${CMD}

echo "Saving current directory..."
CURRENT_DIRECTORY=`pwd`

echo "Changing to temporay directory..."
cd ${TEMP_DIRECTORY}

echo "Removing previous files (${INPUT_FILE}*)..."
rm -f ${INPUT_FILE}*

echo "Compressing input file with gzip..."
cp ${INPUT} .
gzip ${INPUT_FILE}

echo "Compressing input file with bz2..."
cp ${INPUT} .
bzip2 ${INPUT_FILE}

echo "Compressing input file with lz4..."
cp ${INPUT} .
lz4 ${INPUT_FILE}

echo "Compressing input file with lzo..."
cp ${INPUT} .
lzop ${INPUT_FILE}

echo "Removing input file (${INPUT_FILE})..."
rm -f ${INPUT_FILE}

echo "Returning to previous directory..."
cd ${CURRENT_DIRECTORY}

echo "Exporting HADOOP classpath..."
CMD="export HADOOP_CLASSPATH=${JAR_FILE}"
echo -e "\n${CMD}"
${CMD}

echo "Running example on input data..."

ARGS=file://${TEMP_DIRECTORY}
	
CMD="hadoop ${MAIN_CLASS} ${ARGS}"
echo -e "\n${CMD}\n"
${CMD}
