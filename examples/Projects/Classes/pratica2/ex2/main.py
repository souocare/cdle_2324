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
    
    os.system("hadoop fs -rm -r /user" + output)
    os.system("rm -r /home" + output + "/*")

    # remove input folder hadoop
    destination_directory = "/user/usermr/examples/input/gutenberg-small/*" + input
    check_dir_command = "hadoop fs -test -e " + destination_directory
    directory_exists = os.system(check_dir_command) == 0
    if not directory_exists:

        print(f"Destination directory {destination_directory} does not exist in HDFS. Creating it...")
        os.system("hadoop fs -mkdir -p " + destination_directory)


    #remove output folder of user/usermr
    destination_directory_outhome = "/user/usermr/examples/output/"
    check_dir_command_outhome = "hadoop fs -test -e " + destination_directory_outhome
    directory_exists_outhome = os.system(check_dir_command_outhome) == 0
    if not directory_exists_outhome:

        print(f"Destination directory {destination_directory_outhome} does not exist in HDFS. Creating it...")
        os.system("hadoop fs -mkdir -p " + destination_directory_outhome)



    # added file to hadoop
    os.system("hadoop fs -put -f /home/usermr/examples/input/gutenberg-small/" + input + " /user/usermr/examples/input/gutenberg-small/")

    os.system('python3 mapreduce.py -r hadoop hdfs:///user/usermr/examples/input/gutenberg-small/' + input + ' -o /user' + output + ' -nr 2 -cc GzipCodec')

    print("")


    os.system("hadoop fs -copyToLocal /user/usermr/examples/output/ /home/usermr/examples/")

else:

    os.system("rm -r /home/usermr/examples/output/gutenberg-small/*")

    os.system("python3 mapreduce.py file:///home/usermr/examples/input/gutenberg-small/"+input+" -o "+output+" -nr 2 -cc GzipCodec")
    pass
