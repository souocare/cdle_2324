'''
 # @ Description:
 4. Desenvolva dois programas em Java que possam ser utilizados para copiar dados entre o sistema de ficheiros local 
 (tipo de sistema de ficheiros file://) e o sistema de ficheiros HDFS (tipo de sistema de ficheiros hdfs://).
 '''


from subprocess import Popen, PIPE

def copy_to_hdfs(local_path, hdfs_path):
    cmd = f'hadoop fs -copyFromLocal {local_path} {hdfs_path}'
    process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    process.communicate()

def copy_from_hdfs(hdfs_path, local_path):
    cmd = f'hadoop fs -copyToLocal {hdfs_path} {local_path}'
    process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    process.communicate()

if __name__ == '__main__':
    # Exemplo de cópia para o HDFS
    copy_to_hdfs('local_file.txt', 'hdfs:///user/hadoop_user/hdfs_file.txt')

    # Exemplo de cópia do HDFS para local
    copy_from_hdfs('hdfs:///user/hadoop_user/hdfs_file.txt', 'local_file.txt')
