#!/bin/bash

source ./usage.sh > /dev/null

TimeOut=5

function showMessage() {
	echo ""
	read -t ${TimeOut} -p "$1"
	echo ""
}

#
# Hadoop configuration files
#
CFG=""
for conf in "$@"; do
	CFG="${CFG} ${conf}"
done

#
# Hadoop remote user
#
_USER=${USER}

export HADOOP_USER_NAME=${_USER}

CORPUS_NAME=gutenberg-small

#
# Input files
#
BASE_INPUT=examples/input/${CORPUS_NAME}

LOCAL_INPUT_DIRECTORY=${HOME}/${BASE_INPUT}
LOCAL_INPUT=file://${LOCAL_INPUT_DIRECTORY}
INPUT_DIRECTORY=/user/${_USER}/${BASE_INPUT}
INPUT=hdfs://${INPUT_DIRECTORY}

echo -e "\nCreating input directory in HDFS file system..."
CMD="hadoop fs ${CFG} -mkdir -p ${INPUT}"
echo -e "${CMD}"
${CMD}

showMessage "Resuming in ${TimeOut} seconds"

echo -e "\nCopying input files to HDFS file system..."
CMD="hadoop fs ${CFG} -cp -f ${LOCAL_INPUT}/*.* ${INPUT}"
echo -e "${CMD}"
${CMD}

showMessage "Resuming in ${TimeOut} seconds"

#
# Output files
#
BASE_OUTPUT=examples/output/${CORPUS_NAME}
OUTPUT_DIRECTORY=/user/${_USER}/${BASE_OUTPUT}
OUTPUT=hdfs://${OUTPUT_DIRECTORY}

echo -e "\nRemoving previous output..."
CMD="hadoop fs ${CFG} -rm -f -r ${OUTPUT}"
echo -e "${CMD}"
${CMD}

showMessage "Resuming in ${TimeOut} seconds"

#
# Job jar
#
LOCAL_JOB_JAR=${JAR_FILE}

#
# Job additional jars
#
CURRENT_DIRECTORY=`pwd`
LOCAL_ADDITIONAL_CLASSPATH_DIRECTORY=`realpath ${CURRENT_DIRECTORY}/../Ex12-WordCount-02-Lib/target`

LOCAL_JOB_ADDITIONAL_CLASSPATH_FILE=`ls ${LOCAL_ADDITIONAL_CLASSPATH_DIRECTORY}/*.jar`
LOCAL_JOB_ADDITIONAL_CLASSPATH=${LOCAL_JOB_ADDITIONAL_CLASSPATH_FILE}

JOB_ADDITIONAL_CLASSPATH=file://${LOCAL_JOB_ADDITIONAL_CLASSPATH}

#
# ToolRunner and GenericOptionsParser arguments/configuration
#
# Job settings
#


#
# Specify comma separated jar files to include in the classpath. Applies only to the job.
#
# -libjars <comma seperated list of jars>
#
JOB_CLASSPATH="-libjars ${JOB_ADDITIONAL_CLASSPATH}"


#
# Specify application configuration files
#
# -conf <configuration file>
#
JOB_CONF="${CFG}"


#
# Specify comma separated files to be copied to the map reduce cluster. Applies only to the job.
#
# -files <comma separated list of files>
#
JOB_DISTRIBUTED_CACHE="-files patterns.txt,${JOB_ADDITIONAL_CLASSPATH}"

#
# Aggregate of the job properties/configurations
#
JOB_PROPERTIES="${JOB_CLASSPATH} ${JOB_CONF} ${JOB_DISTRIBUTED_CACHE}"

#
# Arguments, including properties/configurations, of the job
#
ARGS="${JOB_PROPERTIES} ${INPUT} ${OUTPUT}"


echo -e "\nExporting classpath..."
CMD="export HADOOP_CLASSPATH=${LOCAL_JOB_ADDITIONAL_CLASSPATH}"
echo ${CMD}
${CMD}

echo -e "\nPress ENTER to run the example..."
read x

echo -e "\nRunning..."
CMD="hadoop jar ${JAR_FILE} ${ARGS}"
echo -e "${CMD}\n"

showMessage "Resuming in ${TimeOut} seconds"

${CMD}

echo -e "\nPress ENTER to view the results..."
read x

CMD="hadoop fs -cat ${OUTPUT_DIRECTORY}/part-r-00000"
echo -e "${CMD}\n"
${CMD}
