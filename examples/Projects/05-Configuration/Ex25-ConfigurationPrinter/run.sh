#!/bin/bash

source ./usage.sh > /dev/null

CFG=""
for conf in "$@"; do
	CFG="${CFG} ${conf}"
done


CMD="unset HADOOP_CLASSPATH"
echo -e "\n${CMD}"
${CMD}

CMD="hadoop jar ${JAR_FILE} ${CFG}"
echo -e "\n${CMD}\n"
${CMD}
