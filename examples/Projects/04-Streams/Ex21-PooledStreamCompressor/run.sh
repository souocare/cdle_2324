#!/bin/bash

source ./usage.sh > /dev/null

declare -a codecs=(
	"org.apache.hadoop.io.compress.GzipCodec" 
	"org.apache.hadoop.io.compress.BZip2Codec"
	"org.apache.hadoop.io.compress.Lz4Codec"
	"com.hadoop.compression.lzo.LzopCodec"
	)
	
declare -a tools=(
	"zcat" 
	"bzip2 -d"
	"lz4 -c"
	"lzop -d -c"
	)

echo "Exporting HADOOP classpath..."
CMD="export HADOOP_CLASSPATH=${JAR_FILE}"
echo -e "\n${CMD}"
${CMD}

codecsLength=${#codecs[@]}

for (( i=0; i<${codecsLength}; i++ ));
do
	ARGS=${codecs[$i]}
	TOOL=${tools[$i]}
	
	CMD="echo \"This is a test\" | hadoop ${MAIN_CLASS} ${ARGS} | ${TOOL}"
	echo -e "\n${CMD}"
	
	echo "This is a test" | hadoop ${MAIN_CLASS} ${ARGS} | ${TOOL}
	
	echo -e "\n\nPress ENTER to continue"
	read x
done
