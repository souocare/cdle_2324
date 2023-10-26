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
    echo ""
    echo "Where <Compression Codec> can be:"
    echo ""
    echo "none - No compression"
    echo "gzip - Gzip compression"
    echo "your_codec - Your custom compression codec"
    exit
}

HOME=/home/usermr
USER=/usermr

if [[ $# -lt 2 ]]; then
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

    # Parse the new parameter
    COMPRESSION_CODEC=$2
fi


NUMBER_REDUCERS=2
NUMBER_DISPLAY_LINES=15

# CORPUS_NAME=gutenberg-mixed
CORPUS_NAME=gutenberg-small

INPUT=${BASE_DIRECTORY}/input/${CORPUS_NAME}
OUTPUT=${BASE_DIRECTORY}/output/${CORPUS_NAME}

# Remove previous output
echo "Removing previous output..."
rm -rf ${OUTPUT}

echo "Creating new folder..."
mkdir ${OUTPUT}

# Define MRJob command
MRJOB_CMD="python3 mapreduce.py"
MRJOB_INPUT="${FILE_SYSTEM_TYPE}${INPUT}"
MRJOB_OUTPUT="${FILE_SYSTEM_TYPE}${OUTPUT}"

# Run MRJob
$MRJOB_CMD -r hadoop $MRJOB_INPUT $MRJOB_OUTPUT 

# Process output files
OUT_FILES="hadoop fs -ls ${MRJOB_OUTPUT} | tr -s ' ' | cut -d' ' -f8"
for file in $OUT_FILES; do
    echo ""
    echo "Result sorted by key - MapReduce defaults - (first $NUMBER_DISPLAY_LINES lines)"
    hadoop fs -text $file 2>/dev/null | head -n $NUMBER_DISPLAY_LINES

    echo ""
    echo "Result sorted (by value) using the linux sort command"
    hadoop fs -text $file 2>/dev/null | sort -k 2,2 -n -r | head -n $NUMBER_DISPLAY_LINES
done
