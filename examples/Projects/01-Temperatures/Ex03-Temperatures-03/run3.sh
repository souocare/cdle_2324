#!/bin/bash

clear

source ./usage.sh

#
# Hadoop configuration files
#
CFG=""
for conf in "$@"; do
	CFG="${CFG} -conf ${conf}"
done

#
# Hadoop remote user
#
_USER=${USER}

export HADOOP_USER_NAME=${_USER}

#
# Input files
#
LOCAL_INPUT_DIRECTORY=${HOME}/examples/input/temperatures/s1-s2
LOCAL_INPUT=file://${LOCAL_INPUT_DIRECTORY}

INPUT_DIRECTORY=/user/${_USER}/examples/input/temperatures/s1-s2
INPUT=hdfs://${INPUT_DIRECTORY}

echo -e "\nCreating input directory in HDFS file system..."
CMD="hadoop fs ${CFG} -mkdir -p ${INPUT}"
echo -e "${CMD}"
${CMD}

echo -e "\nCopying input files to HDFS file system..."
CMD="hadoop fs ${CFG} -cp -f ${LOCAL_INPUT}/*.* ${INPUT}"
echo -e "${CMD}"
${CMD}

#
# Output files
#
OUTPUT_DIRECTORY=/user/${_USER}/examples/output/temperatures/s1-s2
OUTPUT=hdfs://${OUTPUT_DIRECTORY}

echo -e "\nRemoving previous output..."
CMD="hadoop fs ${CFG} -rm -f -r ${OUTPUT}"
echo -e "${CMD}"
${CMD}

#
# Job jar
#
LOCAL_JOB_JAR=${JAR_FILE}

#
# Job additional jars
#
CURRENT_DIRECTORY=`pwd`
ADDITIONAL_CLASSPATH_DIRECTORY=${CURRENT_DIRECTORY}/../Ex04-Temperatures-03-Lib/target
LOCAL_JOB_ADDITIONAL_CLASSPATH=`ls ${ADDITIONAL_CLASSPATH_DIRECTORY}/*.jar`

#
# Job settings
#

JOB_CLASSPATH="-libjars ${LOCAL_JOB_ADDITIONAL_CLASSPATH}"

JOB_CONF="${JOB_CLASSPATH} ${CFG}"

ARGS="${JOB_CONF} ${INPUT} ${OUTPUT}"

echo -e "\nUnsetting HADOOP classpath..."
CMD="unset HADOOP_CLASSPATH"
echo -e "${CMD}"
${CMD}

echo -e "\nRunning..."
CMD="hadoop jar ${LOCAL_JOB_JAR} ${ARGS}"
echo -e "${CMD}\n"

${CMD}

echo "Press ENTER to view result..."
read x

CMD="hadoop fs ${CFG} -cat ${OUTPUT}/part-r-00000"
echo -e "${CMD}\n"
${CMD}
