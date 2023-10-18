import os
import mapreduce
import sys

list_of_arguments = sys.argv
print(list_of_arguments)


localorhadoop = list_of_arguments[1]
input = list_of_arguments[2]
output = list_of_arguments[3]

localorhadoop = "hadoop"

if localorhadoop == "hadoop":

    # added file to hadoop
    os.system("hadoop fs -put -f /home/usermr/examples/input/gutenberg-small/" + input + " /user/usermr/examples/input/gutenberg-small/")

    print('python3 mapreduce.py -r hadoop ' + input)

    os.system('python3 mapreduce.py -r hadoop hdfs:///user/usermr/examples/input/gutenberg-small/' + input + " -output=" + output)

    os.system("hadoop fs -copyToLocal /user/usermr/utput=/home/usermr/examples/output/gutenberg-small/ /home/usermr/examples/output/gutenberg-small/")

else:
    pass
