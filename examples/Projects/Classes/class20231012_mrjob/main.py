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
    # Remove files so it can generate the new ouputs
    os.system("hadoop fs -rm -r /user/usermr/utput=/home/usermr/examples/output/")
    os.system("rm -r /home/usermr/examples/output/gutenberg-small/*")

    # added file to hadoop
    os.system("hadoop fs -put -f /home/usermr/examples/input/gutenberg-small/" + input + " /user/usermr/examples/input/gutenberg-small/")

    print('python3 mapreduce.py -r hadoop hdfs:///user/usermr/examples/input/gutenberg-small/' + input + " -output=" + output + " -nr 2")

    os.system('python3 mapreduce.py -r hadoop hdfs:///user/usermr/examples/input/gutenberg-small/' + input + " -output=" + output + " -nr 2 -cc GzipCodec")

    os.system("hadoop fs -copyToLocal /user/usermr/utput=/home/usermr/examples/output/gutenberg-small/ /home/usermr/examples/output/")

else:
    
    os.system("rm -r /home/usermr/examples/output/gutenberg-small/*")

    os.system("python3 mapreduce.py file:///home/usermr/examples/input/gutenberg-small/"+input+" -o "+output+" -nr 2 -cc GzipCodec")
    pass
