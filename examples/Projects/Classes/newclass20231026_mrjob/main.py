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
    os.system("hadoop fs -rm -r /user/usermr/output")
    os.system("rm -r /home/usermr/examples/output/facedetect/*")

    destination_directory = "/user/usermr/examples/input/facedetect/" + input
    check_dir_command = "hadoop fs -test -e " + destination_directory
    directory_exists = os.system(check_dir_command) == 0

    if not directory_exists:
        print(f"Destination directory {destination_directory} does not exist in HDFS. Creating it...")
        os.system("hadoop fs -mkdir -p " + destination_directory)

    # added file to hadoop
    os.system("hadoop fs -put -f /home/usermr/examples/input/facedetect/" + input + " /user/usermr/examples/input/facedetect/")

    print('python3 mapreduce.py -r hadoop hdfs:///user/usermr/examples/input/facedetect/' + input + ' -output="' + output + '"-nr 2')

    os.system('python3 mapreduce.py -r hadoop hdfs:///user/usermr/examples/input/facedetect/' + input + ' -o ' + output + ' -nr 2')

    print("")
    os.system("hadoop fs -copyToLocal /user/usermr/output/ /home/usermr/examples/")

else:
    
    os.system("rm -r /home/usermr/examples/output/facedetect/*")

    os.system("python3 mapreduce.py file:///home/usermr/examples/input/facedetect/"+input+" -o "+output+" -nr 2")
    pass
