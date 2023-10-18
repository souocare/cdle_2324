#!/bin/bash

function usage() {
	echo "Invalid arguments!"
	echo "Usage:"
	echo "$0 <File System type>"
	echo ""
	echo "Where <File System type> can be:"
	echo ""
	echo "local - local file system (file://)"
	echo "HDFS - HDFS file system (hdfs://)"
	exit
}

if [[ $# -lt 1 ]]; then
	usage
else
	case "$1" in
		"local")
			FILE_SYSTEM_TYPE=file://
			BASE_DIRECTORY=${HOME}/examples
			;;
		
		"HDFS")
			FILE_SYSTEM_TYPE=hdfs://
			BASE_DIRECTORY=/user/${USER}
			;;
		
		*)
			usage
	esac
fi

source ./usage.sh

NUMBER_REDUCERS=2
NUMBER_DISPLAY_LINES=15

#CORPUS_NAME=gutenberg-mixed
CORPUS_NAME=gutenberg-small

INPUT=${BASE_DIRECTORY}/input/${CORPUS_NAME}
OUTPUT=${BASE_DIRECTORY}/output/${CORPUS_NAME}

INPUT_DIRECTORY=${FILE_SYSTEM_TYPE}${INPUT}
OUTPUT_DIRECTORY=${FILE_SYSTEM_TYPE}${OUTPUT}

echo "Removing previous output..."

if [ "${FILE_SYSTEM_TYPE}" == "file://" ]; then
	echo -e "\nRemoving previous output..."
	CMD="rm -rf ${OUTPUT}"
	echo -e "\n${CMD}"
	${CMD}
else
	echo -e "\nCreating input directory in HDFS file system..."
	CMD="hadoop fs -mkdir -p ${INPUT}"
	echo -e "${CMD}"
	${CMD}
	
	LOCAL_INPUT=file://${HOME}/examples/input/${CORPUS_NAME}
	
	echo -e "\nCopying input files to HDFS file system..."
	CMD="hadoop fs -cp -f ${LOCAL_INPUT}/*.* ${INPUT}"
	echo -e "${CMD}"
	${CMD}
	
	echo -e "\nRemoving previous output..."
	CMD="hadoop fs -rm -f -r ${OUTPUT}"
	echo -e "\n${CMD}"
	${CMD}
fi

ARGS="${INPUT_DIRECTORY} ${OUTPUT_DIRECTORY} ${NUMBER_REDUCERS}"

echo "Exporting classpath..."
export HADOOP_CLASSPATH=${JAR_FILE}

echo "HADOOP_CLASSPATH=${HADOOP_CLASSPATH}"

echo "Running..."
CMD="hadoop ${MAIN_CLASS} ${ARGS}"
echo ${CMD}

${CMD}

OUT_FILES=`hadoop fs -ls ${OUTPUT_DIRECTORY}/part-r-* | tr -s ' ' | cut -d' ' -f8`

for file in ${OUT_FILES}; do

	echo ""
		
	echo "Result sorted by key - MapReduce defaults - (first ${NUMBER_DISPLAY_LINES} lines)"
	CMD="hadoop fs -text ${file} 2>/dev/null | head -n ${NUMBER_DISPLAY_LINES}"
	echo ${CMD}
	
	hadoop fs -text ${file} 2>/dev/null | head -n ${NUMBER_DISPLAY_LINES}
	
	echo ""
	echo "Result sorted (by value) using the linux sort command"
	CMD="hadoop fs -text ${file} 2>/dev/null | sort -k 2,2 -n -r | head -n ${NUMBER_DISPLAY_LINES}"
	echo ${CMD}
	
	hadoop fs -text ${file} 2>/dev/null | sort -k 2,2 -n -r | head -n ${NUMBER_DISPLAY_LINES}
done
