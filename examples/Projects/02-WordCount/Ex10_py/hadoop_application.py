#!/usr/bin/env python
import sys
import os

# Check if the correct number of command-line arguments is provided
if len(sys.argv) < 3:
    print("Usage: hadoop ... <input path> <output path> [number of reducers] [compression codec]")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

# Check if the input path exists
if not os.path.exists(input_path):
    print("Input path does not exist.")
    sys.exit(1)

# Set the default number of reducers
num_reducers = 1

# Check if the number of reducers is provided as an argument
if len(sys.argv) > 3:
    try:
        num_reducers = int(sys.argv[3])
    except ValueError:
        print("Invalid number of reducers. Using the default value (1).")

# Set the default compression codec
compression_codec = "none"

# Check if the compression codec is provided as an argument
if len(sys.argv) > 4:
    compression_codec = sys.argv[4].lower()

# Check if the compression codec is gzip
if compression_codec != "gzip":
    print("Invalid compression codec. Only 'gzip' is supported.")
    sys.exit(1)

# Run Hadoop Streaming job with the provided arguments
hadoop_streaming_command = (
    f"hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar "
    f"-files mapper.py,reducer.py "
    f"-mapper ./mapper.py "
    f"-reducer ./reducer.py "
    f"-input {input_path} "
    f"-output {output_path} "
    f"-numReduceTasks {num_reducers}"
)

os.system(hadoop_streaming_command)
