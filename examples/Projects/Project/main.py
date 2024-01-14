import os
import sys

def get_user_choice():
    print("Choose an operation:")
    print("1. Word Count TXT")
    print("2. Word Count JSON")
    print("3. Word Count JSON REGEX")
    print("4. Frequency Table Bigrams")
    print("5. Singletons - Bigrams")
    print("6. Singletons")
    print("7. TF-IDF")
    print("8. Sentiment Analysis with Model")
    print("9. Sentiment Analysis (Dictionary)")
    print("10. Video Analysis")

    choice = input("Enter the number corresponding to your choice: ")
    if str(choice) in ["9", "10"]:
        print("Estas tarefas não estão a funcionar correctamente. Poderá correr com o comando apresentado no ficheiro README.")
        sys.exit()
    if str(choice) not in ["1", "2", "3", "4", "5", "6", "7", "8"]:
        print("Invalid choice. Please run again, and enter a number between 1 and 10.")
        sys.exit()
    return str(choice)


choices_paths = {"1": "1_word_count_txt/mapreduce.py", 
                 "2": "2_word_count_json/mapreduce.py", 
                 "3": "3_word_count_json_regex/mapreduce.py", 
                 "4": "4_frequency_table_bigrams/mapreduce.py", 
                 "5": "5_bigram_singletons/mapreduce.py", 
                 "6": "6_singletons/mapreduce.py", 
                 "7": "7_tfidf/mapreduce.py", 
                 "8": "8_sentimento_model/mapreduce.py", 
                 "9": "9_sentimento_dict/mapreduce.py", 
                 "10": "10_video/mapreduce.py"}

print(choices_paths["1"].split("/")[1])

list_of_arguments = sys.argv

selected_operation = get_user_choice()
list_of_arguments.append(selected_operation)
dirname_main, filename_main = os.path.split(os.path.abspath(__file__))

print("List of arguments:")
print(list_of_arguments)


localorhadoop = list_of_arguments[1]
inputfile = list_of_arguments[2]
output = list_of_arguments[3]
pythonfilename = choices_paths[selected_operation]
nrreducers = list_of_arguments[4]
compressioncodec = list_of_arguments[5]
compressioncodec = "GzipCodec" # assuming it's always gzip to make things easier

#localorhadoop = "hadoop"

if localorhadoop == "hadoop":
    # Remove files so it can generate the new ouputs
    destination_directory = "/user/usermr/projecttextanalysis/input/" + choices_paths[selected_operation].split("/")[0]
    destination_directory_outhome = "/user/usermr/projecttextanalysis/output/" + choices_paths[selected_operation].split("/")[0]
    os.system("hadoop fs -rm -r " + destination_directory_outhome)
    print("CMD: hadoop fs -rm -r " + destination_directory_outhome)
    os.system("rm -r " + output + "*")
    print("CMD: rm -r " + output + "*")
    
    check_dir_command = "hadoop fs -test -e " + destination_directory
    directory_exists = os.system(check_dir_command) == 0
    if not directory_exists:

        print(f"Destination directory {destination_directory} does not exist in HDFS. Creating it...")
        os.system("hadoop fs -mkdir -p " + destination_directory)

    # added file to hadoop
    os.system("hadoop fs -put -f " + inputfile + " " + destination_directory)

    os.system('python3 ' + dirname_main + "/" + pythonfilename + ' -r hadoop hdfs://'  + destination_directory + "/" + inputfile.split("/")[-1] + ' -o /' + destination_directory_outhome + "/" + ' -nr ' + nrreducers + ' -cc ' + compressioncodec)
    
    os.system("hadoop fs -copyToLocal " +  destination_directory_outhome + " " + output + choices_paths[selected_operation].split("/")[0])

else:
    
    os.system("rm -r " + output + "*")
    if "/" in inputfile:
        os.system("python3 "+ dirname_main + "/" + pythonfilename + " file://"+inputfile +" -o "+output + choices_paths[selected_operation].split("/")[0] + "/" +" -nr "  + nrreducers + ' -cc ' + compressioncodec)
    else:
        os.system("python3 "+ dirname_main + "/" + pythonfilename + " file://"+ dirname_main + "/" + inputfile +" -o "+output + choices_paths[selected_operation].split("/")[0] + "/" +" -nr "  + nrreducers + ' -cc ' + compressioncodec)
